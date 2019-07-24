﻿// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using EnsureThat;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Health.Extensions.DependencyInjection;
using Microsoft.Health.Fhir.Core.Configs;
using Microsoft.Health.Fhir.Core.Features.Operations.Export.ExportDestinationClient;
using Microsoft.Health.Fhir.Core.Features.Operations.Export.Models;
using Microsoft.Health.Fhir.Core.Features.Persistence;
using Microsoft.Health.Fhir.Core.Features.Search;
using Microsoft.Health.Fhir.Core.Features.SecretStore;
using Newtonsoft.Json;
using Task = System.Threading.Tasks.Task;

namespace Microsoft.Health.Fhir.Core.Features.Operations.Export
{
    public class ExportJobTask : IExportJobTask
    {
        private readonly Func<IScoped<IFhirOperationDataStore>> _fhirOperationDataStoreFactory;
        private readonly ISecretStore _secretStore;
        private readonly ExportJobConfiguration _exportJobConfiguration;
        private readonly Func<IScoped<ISearchService>> _searchServiceFactory;
        private readonly IResourceToByteArraySerializer _resourceToByteArraySerializer;
        private readonly IExportDestinationClientFactory _exportDestinationClientFactory;
        private readonly ILogger _logger;

        // Currently we will have only one file per resource type. In the future we will add the ability to split
        // individual files based on a max file size. This could result in a single resource having multiple files.
        // We will have to update the below mapping to support multiple ExportFileInfo per resource type.
        private readonly IDictionary<string, ExportFileInfo> _resourceTypeToFileInfoMapping = new Dictionary<string, ExportFileInfo>();

        private ExportJobRecord _exportJobRecord;
        private WeakETag _weakETag;
        private IExportDestinationClient _exportDestinationClient;

        public ExportJobTask(
            Func<IScoped<IFhirOperationDataStore>> fhirOperationDataStoreFactory,
            ISecretStore secretStore,
            IOptions<ExportJobConfiguration> exportJobConfiguration,
            Func<IScoped<ISearchService>> searchServiceFactory,
            IResourceToByteArraySerializer resourceToByteArraySerializer,
            IExportDestinationClientFactory exportDestinationClientFactory,
            ILogger<ExportJobTask> logger)
        {
            EnsureArg.IsNotNull(fhirOperationDataStoreFactory, nameof(fhirOperationDataStoreFactory));
            EnsureArg.IsNotNull(secretStore, nameof(secretStore));
            EnsureArg.IsNotNull(exportJobConfiguration?.Value, nameof(exportJobConfiguration));
            EnsureArg.IsNotNull(searchServiceFactory, nameof(searchServiceFactory));
            EnsureArg.IsNotNull(resourceToByteArraySerializer, nameof(resourceToByteArraySerializer));
            EnsureArg.IsNotNull(exportDestinationClientFactory, nameof(exportDestinationClientFactory));
            EnsureArg.IsNotNull(logger, nameof(logger));

            _fhirOperationDataStoreFactory = fhirOperationDataStoreFactory;
            _secretStore = secretStore;
            _exportJobConfiguration = exportJobConfiguration.Value;
            _searchServiceFactory = searchServiceFactory;
            _resourceToByteArraySerializer = resourceToByteArraySerializer;
            _exportDestinationClientFactory = exportDestinationClientFactory;
            _logger = logger;
        }

        /// <inheritdoc />
        public async Task ExecuteAsync(ExportJobRecord exportJobRecord, WeakETag weakETag, CancellationToken cancellationToken)
        {
            EnsureArg.IsNotNull(exportJobRecord, nameof(exportJobRecord));

            _exportJobRecord = exportJobRecord;
            _weakETag = weakETag;

            try
            {
                // Get destination type from secret store and connect to the destination using appropriate client.
                await GetDestinationInfoAndConnectAsync(cancellationToken);

                // If we are resuming a job, we can detect that by checking the progress info from the job record.
                // If it is null, then we know we are processing a new job.
                if (_exportJobRecord.Progress == null)
                {
                    _exportJobRecord.Progress = new ExportJobProgress(continuationToken: null, page: 0);
                }

                ExportJobProgress progress = _exportJobRecord.Progress;

                // Current batch will be used to organize a set of search results into a group so that they can be committed together.
                uint currentBatchId = progress.Page;

                // The first item is placeholder for continuation token so that it can be updated efficiently later.
                var queryParameters = new Tuple<string, string>[]
                {
                    Tuple.Create(KnownQueryParameterNames.ContinuationToken, progress.ContinuationToken),
                    Tuple.Create(KnownQueryParameterNames.Count, _exportJobConfiguration.MaximumNumberOfResourcesPerQuery.ToString(CultureInfo.InvariantCulture)),
                    Tuple.Create(KnownQueryParameterNames.LastUpdated, $"le{_exportJobRecord.QueuedTime.ToString("o", CultureInfo.InvariantCulture)}"),
                };

                // Process the export if:
                // 1. There is continuation token, which means there is more resource to be exported.
                // 2. There is no continuation token but the page is 0, which means it's the initial export.
                while (progress.ContinuationToken != null || progress.Page == 0)
                {
                    SearchResult searchResult;

                    // Search and process the results.
                    using (IScoped<ISearchService> searchService = _searchServiceFactory())
                    {
                        // If the continuation token is null, then we will exclude it. Calculate the offset and count to be passed in.
                        int offset = queryParameters[0].Item2 == null ? 1 : 0;

                        searchResult = await searchService.Value.SearchAsync(
                            _exportJobRecord.ResourceType,
                            new ArraySegment<Tuple<string, string>>(queryParameters, offset, queryParameters.Length - offset),
                            cancellationToken);
                    }

                    await ProcessSearchResultsAsync(searchResult.Results, currentBatchId, cancellationToken);

                    if (searchResult.ContinuationToken == null)
                    {
                        // No more continuation token, we are done.
                        break;
                    }

                    // Update the continuation token (local cache).
                    progress.UpdateContinuationToken(searchResult.ContinuationToken);
                    queryParameters[0] = Tuple.Create(KnownQueryParameterNames.ContinuationToken, progress.ContinuationToken);

                    // The export job can be canceled while the job is executing. We will detect that in two ways:
                    // 1. During update of the job record after the changes have been committed.
                    // 2. During periodic check of the job record.
                    //
                    // If the cancellation is detected during step 1, it means the Output in the persisted job record is out of sync since it failed to update the job record.
                    // In that case, we would want to update the job record so that it matches (best effort) what's been actually committed.
                    // If the cancellation is detected during step 2, we don't need to update the job record even though there are new resources
                    // exported because those resources haven't been committed yet and therefore the user will not see them.
                    if (progress.Page % _exportJobConfiguration.NumberOfPagesPerCommit == 0)
                    {
                        // Commit the changes.
                        await _exportDestinationClient.CommitAsync(cancellationToken);

                        // Update the job record.
                        try
                        {
                            await UpdateJobRecordAsync(cancellationToken);
                        }
                        catch (JobConflictException)
                        {
                            await HandleExportJobUpdatedExternallyAsync(updatedExportJob: null, updateJobRecord: true, cancellationToken);

                            return;
                        }

                        currentBatchId = progress.Page;
                    }
                    else
                    {
                        bool updated = false;
                        ExportJobOutcome updatedExportJob = null;

                        // Check to see if the job has been canceled.
                        using (IScoped<IFhirOperationDataStore> fhirOperationDataStore = _fhirOperationDataStoreFactory())
                        {
                            (updated, updatedExportJob) = await fhirOperationDataStore.Value.TryGetUpdatedExportJobAsync(_exportJobRecord.Id, _weakETag, cancellationToken);
                        }

                        if (updated)
                        {
                            await HandleExportJobUpdatedExternallyAsync(updatedExportJob, updateJobRecord: false, cancellationToken);

                            return;
                        }
                    }
                }

                // Commit one last time for any pending changes.
                await _exportDestinationClient.CommitAsync(cancellationToken);

                await CompleteJobAsync(OperationStatus.Completed, cancellationToken);

                _logger.LogTrace("Successfully completed the job.");

                try
                {
                    // Best effort to delete the secret. If it fails to delete, then move on.
                    await _secretStore.DeleteSecretAsync(_exportJobRecord.SecretName, cancellationToken);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Failed to delete the secret.");
                }
            }
            catch (Exception ex)
            {
                // The job has encountered an error it cannot recover from.
                // Try to update the job to failed state.
                _logger.LogError(ex, "Encountered an unhandled exception. The job will be marked as failed.");

                await CompleteJobAsync(OperationStatus.Failed, cancellationToken);
            }
        }

        private async Task CompleteJobAsync(OperationStatus completionStatus, CancellationToken cancellationToken)
        {
            _exportJobRecord.Status = completionStatus;
            _exportJobRecord.EndTime = Clock.UtcNow;

            await UpdateJobRecordAsync(cancellationToken);
        }

        private async Task UpdateJobRecordAsync(CancellationToken cancellationToken)
        {
            using (IScoped<IFhirOperationDataStore> fhirOperationDataStore = _fhirOperationDataStoreFactory())
            {
                ExportJobOutcome updatedExportJobOutcome = await fhirOperationDataStore.Value.UpdateExportJobAsync(_exportJobRecord, _weakETag, cancellationToken);

                _exportJobRecord = updatedExportJobOutcome.JobRecord;
                _weakETag = updatedExportJobOutcome.ETag;
            }
        }

        // Get destination info from secret store, create appropriate export client and connect to destination.
        private async Task GetDestinationInfoAndConnectAsync(CancellationToken cancellationToken)
        {
            SecretWrapper secret = await _secretStore.GetSecretAsync(_exportJobRecord.SecretName, cancellationToken);

            DestinationInfo destinationInfo = JsonConvert.DeserializeObject<DestinationInfo>(secret.SecretValue);

            _exportDestinationClient = _exportDestinationClientFactory.Create(destinationInfo.DestinationType);

            await _exportDestinationClient.ConnectAsync(destinationInfo.DestinationConnectionString, cancellationToken, _exportJobRecord.Id);
        }

        private async Task ProcessSearchResultsAsync(IEnumerable<ResourceWrapper> searchResults, uint partId, CancellationToken cancellationToken)
        {
            foreach (ResourceWrapper resourceWrapper in searchResults)
            {
                string resourceType = resourceWrapper.ResourceTypeName;

                // Check whether we already have an existing file for the current resource type.
                if (!_resourceTypeToFileInfoMapping.TryGetValue(resourceType, out ExportFileInfo exportFileInfo))
                {
                    // Check whether we have seen this file previously (in situations where we are resuming an export)
                    if (_exportJobRecord.Output.TryGetValue(resourceType, out exportFileInfo))
                    {
                        // A file already exists for this resource type. Let us open the file on the client.
                        await _exportDestinationClient.OpenFileAsync(exportFileInfo.FileUri, cancellationToken);
                    }
                    else
                    {
                        // File does not exist. Create it.
                        string fileName = resourceType + ".ndjson";
                        Uri fileUri = await _exportDestinationClient.CreateFileAsync(fileName, cancellationToken);

                        exportFileInfo = new ExportFileInfo(resourceType, fileUri, sequence: 0);

                        // Since we created a new file the JobRecord Output also needs to know about it.
                        _exportJobRecord.Output.TryAdd(resourceType, exportFileInfo);
                    }

                    _resourceTypeToFileInfoMapping.Add(resourceType, exportFileInfo);
                }

                // Serialize into NDJson and write to the file.
                byte[] bytesToWrite = _resourceToByteArraySerializer.Serialize(resourceWrapper);

                await _exportDestinationClient.WriteFilePartAsync(exportFileInfo.FileUri, partId, bytesToWrite, cancellationToken);

                // Increment the file information.
                exportFileInfo.IncrementCount(bytesToWrite.Length);
            }
        }

        private async Task HandleExportJobUpdatedExternallyAsync(ExportJobOutcome updatedExportJob, bool updateJobRecord, CancellationToken cancellationToken)
        {
            _logger.LogTrace("The job was updated by external process.");

            try
            {
                // Load the latest record if need to.
                if (updatedExportJob == null)
                {
                    using (IScoped<IFhirOperationDataStore> fhirOperationDataStore = _fhirOperationDataStoreFactory())
                    {
                        updatedExportJob = await fhirOperationDataStore.Value.GetExportJobByIdAsync(_exportJobRecord.Id, cancellationToken);
                    }
                }

                if (updatedExportJob.JobRecord.Status == OperationStatus.Canceled)
                {
                    _logger.LogInformation("The job was canceled by another process.");

                    if (updateJobRecord)
                    {
                        // The job was canceled. Merge the output without updating the status.
                        updatedExportJob.JobRecord.Output.Clear();

                        foreach (KeyValuePair<string, ExportFileInfo> kvp in _exportJobRecord.Output)
                        {
                            updatedExportJob.JobRecord.Output.Add(kvp);
                        }

                        _exportJobRecord = updatedExportJob.JobRecord;
                        _weakETag = updatedExportJob.ETag;

                        await UpdateJobRecordAsync(cancellationToken);
                    }
                }
                else
                {
                    _logger.LogWarning("The job was updated by another process but was not canceled. Abandoning the job.");
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to handle the job that was updated externally. The output might not reflect the latest change.");
            }
        }
    }
}
