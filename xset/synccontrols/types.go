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

import (
	"time"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	appsv1alpha1 "kusionstack.io/kube-api/apps/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"kusionstack.io/kube-utils/xset/api"
)

type SyncContext struct {
	Revisions           []*appsv1.ControllerRevision
	CurrentRevision     *appsv1.ControllerRevision
	UpdatedRevision     *appsv1.ControllerRevision
	ExistingSubResource []client.Object

	FilteredTarget []client.Object
	TargetWrappers []targetWrapper
	activeTargets  []*targetWrapper
	replacingMap   map[string]*targetWrapper

	CurrentIDs sets.Int
	OwnedIds   map[int]*appsv1alpha1.ContextDetail

	NewStatus *api.XSetStatus
}

type targetWrapper struct {
	// parameters must be set during creation
	client.Object
	ID            int
	ContextDetail *appsv1alpha1.ContextDetail
	PlaceHolder   bool

	ToDelete  bool
	ToExclude bool

	IsDuringScaleInOps bool
	IsDuringUpdateOps  bool
}

type targetUpdateInfo struct {
	*targetWrapper

	UpdatedTarget client.Object

	InPlaceUpdateSupport bool
	OnlyMetadataChanged  bool

	// indicate if this target has up-to-date revision from its owner, like XSet
	IsUpdatedRevision bool
	// carry the target's current revision
	CurrentRevision *appsv1.ControllerRevision
	// carry the desired update revision
	UpdateRevision *appsv1.ControllerRevision

	// indicates the TargetOpsLifecycle is started.
	IsDuringOps bool
	// indicates operate is allowed for TargetOpsLifecycle.
	IsAllowOps bool
	// requeue after for operationDelaySeconds
	RequeueForOperationDelay *time.Duration

	// for replace update
	// judge target in replace updating
	IsInReplacing bool

	// replace new created target
	ReplacePairNewTargetInfo *targetUpdateInfo

	// replace origin target
	ReplacePairOriginTargetName string
}
