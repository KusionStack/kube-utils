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

package synccontrols

// XSet labels
var (
	// TargetInstanceIDLabelKey is used to attach target instance ID on target
	TargetInstanceIDLabelKey = "xset.kusionstack.io/instance-id"
	// XSetUpdateIndicateLabelKey is used to indicate a target should be updated by label
	XSetUpdateIndicateLabelKey = "xset.kusionstack.io/update-included"

	// TargetDeletionIndicationLabelKey indicates a target will be deleted by xset
	TargetDeletionIndicationLabelKey = "xset.kusionstack.io/to-delete"
	// TargetReplaceIndicationLabelKey indicates a target will be replaced by xset
	TargetReplaceIndicationLabelKey = "xset.kusionstack.io/to-replace"
	// TargetReplaceByReplaceUpdateLabelKey indicates a target is replaced by update by xset
	TargetReplaceByReplaceUpdateLabelKey = "xset.kusionstack.io/replaced-by-replace-update"
	// TargetExcludeIndicationLabelKey indicates a target will be excluded by xset
	TargetExcludeIndicationLabelKey = "xset.kusionstack.io/to-exclude"

	// TargetReplacePairOriginName is used to indicate replace origin target name on the new created target
	TargetReplacePairOriginName = "xset.kusionstack.io/replace-pair-origin-name"
	// TargetReplacePairNewId is used to indicate the new created target instance on replace origin target
	TargetReplacePairNewId = "xset.kusionstack.io/replace-pair-new-id"

	// TargetOrphanedIndicateLabelKey indicates target is orphaned
	TargetOrphanedIndicateLabelKey = "xset.kusionstack.io/orphaned"
)

var (
	LastTargetStatusAnnotationKey = "xset.kusionstack.io/last-target-status"
)
