// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Threading;
using System.Threading.Tasks;
using EnsureThat;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Health.Fhir.Core;

namespace Microsoft.Health.Fhir.Api.Modules.HealthChecks
{
    internal sealed class CachedHealthCheck : IHealthCheck, IDisposable
    {
        private readonly IServiceProvider _provider;
        private readonly Func<IServiceProvider, IHealthCheck> _healthCheck;
        private DateTimeOffset _lastChecked;
        private HealthCheckResult _lastResult;
        private readonly SemaphoreSlim _semaphore = new SemaphoreSlim(1, 1);
        private readonly TimeSpan _cacheTime = TimeSpan.FromSeconds(1);

        public CachedHealthCheck(IServiceProvider provider, Func<IServiceProvider, IHealthCheck> healthCheck)
        {
            EnsureArg.IsNotNull(provider, nameof(provider));
            EnsureArg.IsNotNull(healthCheck, nameof(healthCheck));

            _provider = provider;
            _healthCheck = healthCheck;
        }

        public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken)
        {
            if (_lastChecked >= Clock.UtcNow.Add(-_cacheTime))
            {
                return _lastResult;
            }

            await _semaphore.WaitAsync(cancellationToken);
            try
            {
                if (_lastChecked >= Clock.UtcNow.Add(-_cacheTime))
                {
                    return _lastResult;
                }

                using (var scope = _provider.CreateScope())
                {
                    var check = _healthCheck.Invoke(scope.ServiceProvider);

                    _lastResult = await check.CheckHealthAsync(context, cancellationToken);
                    _lastChecked = Clock.UtcNow;
                }

                return _lastResult;
            }
            finally
            {
                _semaphore.Release();
            }
        }

        public void Dispose()
        {
            _semaphore?.Dispose();
        }
    }
}
