/*
 * Copyright 2024-2025 KusionStack Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package opslifecycle

// TargetOpsLifecycle labels
var (
	// TargetOperatingLabelPrefix indicates a Target is operating
	TargetOperatingLabelPrefix = "operating.opslifecycle.kusionstack.io"
	// TargetOperationTypeLabelPrefix indicates the type of operation
	TargetOperationTypeLabelPrefix = "operation-type.opslifecycle.kusionstack.io"
	// TargetOperationPermissionLabelPrefix indicates the permission of operation
	TargetOperationPermissionLabelPrefix = "operation-permission.opslifecycle.kusionstack.io"
	// TargetUndoOperationTypeLabelPrefix indicates the type of operation has been canceled
	TargetUndoOperationTypeLabelPrefix = "undo-operation-type.opslifecycle.kusionstack.io"
	// TargetDoneOperationTypeLabelPrefix indicates the type of operation has been done
	TargetDoneOperationTypeLabelPrefix = "done-operation-type.opslifecycle.kusionstack.io"

	// TargetPreCheckLabelPrefix indicates a Target is in pre-check phase
	TargetPreCheckLabelPrefix = "pre-check.opslifecycle.kusionstack.io"
	// TargetPreCheckedLabelPrefix indicates a Target has finished pre-check phase
	TargetPreCheckedLabelPrefix = "pre-checked.opslifecycle.kusionstack.io"
	// TargetPreparingLabelPrefix indicates a Target is preparing for operation
	TargetPreparingLabelPrefix = "preparing.opslifecycle.kusionstack.io"
	// TargetOperateLabelPrefix indicates a Target is in operate phase
	TargetOperateLabelPrefix = "operate.opslifecycle.kusionstack.io"
	// TargetOperatedLabelPrefix indicates a Target has finished operate phase
	TargetOperatedLabelPrefix = "operated.opslifecycle.kusionstack.io"
	// TargetPostCheckLabelPrefix indicates a Target is in post-check phase
	TargetPostCheckLabelPrefix = "post-check.opslifecycle.kusionstack.io"
	// TargetPostCheckedLabelPrefix indicates a Target has finished post-check phase
	TargetPostCheckedLabelPrefix = "post-checked.opslifecycle.kusionstack.io"
	// TargetCompletingLabelPrefix indicates a Target is completing operation
	TargetCompletingLabelPrefix = "completing.opslifecycle.kusionstack.io"

	// TargetServiceAvailableLabel indicates a Target is available to serve
	TargetServiceAvailableLabel = "opslifecycle.kusionstack.io/service-available"
	TargetPreCheckLabel         = "opslifecycle.kusionstack.io/pre-checking"
	TargetPreparingLabel        = "opslifecycle.kusionstack.io/preparing"
	TargetOperatingLabel        = "opslifecycle.kusionstack.io/operating"
	TargetPostCheckLabel        = "opslifecycle.kusionstack.io/post-checking"
	TargetCompletingLabel       = "opslifecycle.kusionstack.io/completing"
	TargetCreatingLabel         = "opslifecycle.kusionstack.io/creating"

	// TargetStayOfflineLabel indicates a Target is not ready and available to serve
	TargetStayOfflineLabel     = "opslifecycle.kusionstack.io/stay-offline"
	TargetPreparingDeleteLabel = "opslifecycle.kusionstack.io/preparing-to-delete"
)

var WellKnownLabelPrefixesWithID = []string{
	TargetOperatingLabelPrefix,
	TargetOperationTypeLabelPrefix,
	TargetPreCheckLabelPrefix,
	TargetPreCheckedLabelPrefix,
	TargetPreparingLabelPrefix,
	TargetDoneOperationTypeLabelPrefix,
	TargetUndoOperationTypeLabelPrefix,
	TargetOperateLabelPrefix,
	TargetOperatedLabelPrefix,
	TargetPostCheckLabelPrefix,
	TargetPostCheckedLabelPrefix,
	TargetCompletingLabelPrefix,
}
