﻿// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Diagnostics;
using Microsoft.Health.Fhir.Core.Models;
using Microsoft.Health.Fhir.ValueSets;

namespace Microsoft.Health.Fhir.Core.Exceptions
{
    public class PreconditionFailedException : FhirException
    {
        public PreconditionFailedException(string message)
            : base(message)
        {
            Debug.Assert(!string.IsNullOrEmpty(message), "Exception message should not be empty");

            Issues.Add(new OperationOutcomeIssue(
                IssueSeverity.Error,
                IssueType.Required,
                message));
        }
    }
}
