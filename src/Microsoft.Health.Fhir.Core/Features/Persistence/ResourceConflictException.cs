﻿// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Diagnostics;
using Microsoft.Health.Fhir.Core.Exceptions;
using Microsoft.Health.Fhir.Core.Models;
using Microsoft.Health.Fhir.ValueSets;

namespace Microsoft.Health.Fhir.Core.Features.Persistence
{
    public class ResourceConflictException : FhirException
    {
        public ResourceConflictException(WeakETag etag)
        {
            Debug.Assert(etag != null, "ETag should not be null");

            Issues.Add(new OperationOutcomeIssue(
                IssueSeverity.Error,
                IssueType.Conflict,
                string.Format(Core.Resources.ResourceVersionConflict, etag?.VersionId)));
        }
    }
}
