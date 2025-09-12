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

package api

// +k8s:deepcopy-gen=file

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type XSetConditionType string

const (
	XSetScale  XSetConditionType = "Scale"
	XSetUpdate XSetConditionType = "Update"
)

type XSetSpec struct {
	// Indicates that the scaling and updating is paused and will not be processed by the
	// XSet controller.
	// +optional
	Paused bool `json:"paused,omitempty"`
	// Replicas is the desired number of replicas of the given Template.
	// These are replicas in the sense that they are instantiations of the
	// same Template, but individual replicas also have a consistent identity.
	// If unspecified, defaults to 0.
	// +optional
	Replicas *int32 `json:"replicas,omitempty"`

	// Selector is a label query over targets that should match the replica count.
	// It must match the target template's labels.
	Selector *metav1.LabelSelector `json:"selector,omitempty"`

	// UpdateStrategy indicates the XSetUpdateStrategy that will be
	// employed to update Targets in the XSet when a revision is made to
	// Template.
	// +optional
	UpdateStrategy UpdateStrategy `json:"updateStrategy,omitempty"`

	// ScaleStrategy indicates the strategy detail that will be used during targets scaling.
	// +optional
	ScaleStrategy ScaleStrategy `json:"scaleStrategy,omitempty"`

	// NamigPolicy indicates the strategy detail that will be used for replica naming
	// +optional
	NamingStrategy *NamingStrategy `json:"namingStrategy,omitempty"`

	// Indicate the number of histories to be conserved
	// If unspecified, defaults to 20
	// +optional
	HistoryLimit int32 `json:"historyLimit,omitempty"`
}

type ByPartition struct {
	// Partition controls the number of targets in old revisions.
	// Defaults to nil (all targets will be updated)
	// +optional
	Partition *int32 `json:"partition,omitempty"`
}

type ByLabel struct{}

// RollingUpdateStrategy is used to communicate parameter for rolling update.
type RollingUpdateStrategy struct {
	// ByPartition indicates the update progress is controlled by partition value.
	// +optional
	ByPartition *ByPartition `json:"byPartition,omitempty"`

	// ByLabel indicates the update progress is controlled by attaching target label.
	// +optional
	ByLabel *ByLabel `json:"byLabel,omitempty"`
}

type UpdateStrategy struct {
	// RollingUpdate is used to communicate parameters when Type is RollingUpdateStatefulSetStrategyType.
	// +optional
	RollingUpdate *RollingUpdateStrategy `json:"rollingUpdate,omitempty"`

	// UpdatePolicy indicates the policy by to update targets.
	// +optional
	UpdatePolicy UpdateStrategyType `json:"upgradePolicy,omitempty"`

	// OperationDelaySeconds indicates how many seconds it should delay before operating update.
	// +optional
	OperationDelaySeconds *int32 `json:"operationDelaySeconds,omitempty"`
}

type ScaleStrategy struct {
	// Context indicates the pool from which to allocate Target instance ID.
	// XSets are allowed to share the same Context.
	// It is not allowed to change.
	// Context defaults to be XSet's name.
	// +optional
	Context string `json:"context,omitempty"`

	// TargetToExclude indicates the syncContext which will be orphaned by XSet.
	// +optional
	TargetToExclude []string `json:"targetToExclude,omitempty"`

	// TargetToInclude indicates the syncContext which will be adapted by XSet.
	// +optional
	TargetToInclude []string `json:"targetToInclude,omitempty"`

	// TargetToDelete indicates the syncContext which will be deleted by XSet.
	// +optional
	TargetToDelete []string `json:"targetToDelete,omitempty"`

	// OperationDelaySeconds indicates how many seconds it should delay before operating scale.
	// +optional
	OperationDelaySeconds *int32 `json:"operationDelaySeconds,omitempty"`
}

// TargetNamingSuffixPolicy indicates how a new pod name suffix part is generated.
type TargetNamingSuffixPolicy string

const (
	// TargetNamingSuffixPolicyPersistentSequence uses persistent sequential numbers as pod name suffix.
	TargetNamingSuffixPolicyPersistentSequence TargetNamingSuffixPolicy = "PersistentSequence"
	// TargetNamingSuffixPolicyRandom uses collaset name as pod generateName, which is the prefix
	// of pod name. Kubernetes then adds a random string as suffix after the generateName.
	// This is defaulting policy.
	TargetNamingSuffixPolicyRandom TargetNamingSuffixPolicy = "Random"
)

type NamingStrategy struct {
	// TargetNamingSuffixPolicy is a string enumeration that determaines how pod name suffix will be generated.
	// A collaset pod name contains two parts to be placed in a string formation %s-%s; the prefix is collaset
	// name, and the suffix is determined by TargetNamingSuffixPolicy.
	TargetNamingSuffixPolicy TargetNamingSuffixPolicy `json:"TargetNamingSuffixPolicy,omitempty"`
}

// UpdateStrategyType is a string enumeration type that enumerates
// all possible ways we can update a Target when updating application
type UpdateStrategyType string

const (
	// XSetRecreateTargetUpdateStrategyType indicates that XSet will always update Target by deleting and recreate it.
	XSetRecreateTargetUpdateStrategyType UpdateStrategyType = "Recreate"
	// XSetInPlaceIfPossibleTargetUpdateStrategyType indicates thath XSet will try to update Target by in-place update
	// when it is possible. Recently, only Target image can be updated in-place. Any other Target spec change will make the
	// policy fall back to XSetRecreateTargetUpdateStrategyType.
	XSetInPlaceIfPossibleTargetUpdateStrategyType UpdateStrategyType = "InPlaceIfPossible"
	// XSetInPlaceOnlyTargetUpdateStrategyType indicates that XSet will always update Target in-place, instead of
	// recreating target. It will encounter an error on original Kubernetes cluster.
	XSetInPlaceOnlyTargetUpdateStrategyType UpdateStrategyType = "InPlaceOnly"
	// XSetReplaceTargetUpdateStrategyType indicates that XSet will always update Target by replace, it will
	// create a new Target and delete the old target when the new one service available.
	XSetReplaceTargetUpdateStrategyType UpdateStrategyType = "Replace"
)

type XSetStatus struct {
	// ObservedGeneration is the most recent generation observed for this XSet. It corresponds to the
	// XSet's generation, which is updated on mutation by the API Server.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// CurrentRevision, if not empty, indicates the version of the XSet.
	// +optional
	CurrentRevision string `json:"currentRevision,omitempty"`

	// UpdatedRevision, if not empty, indicates the version of the XSet currently updated.
	// +optional
	UpdatedRevision string `json:"updatedRevision,omitempty"`

	// Count of hash collisions for the XSet. The XSet controller
	// uses this field as a collision avoidance mechanism when it needs to
	// create the name for the newest ControllerRevision.
	// +optional
	CollisionCount *int32 `json:"collisionCount,omitempty"`

	// ReadyReplicas indicates the number of the target with ready condition
	// +optional
	ReadyReplicas int32 `json:"readyReplicas,omitempty"`

	// Replicas is the most recently observed number of replicas.
	// +optional
	Replicas int32 `json:"replicas,omitempty"`

	// the number of scheduled replicas for the replicas set.
	// +optional
	ScheduledReplicas int32 `json:"scheduledReplicas,omitempty"`

	// The number of targets in updated version.
	// +optional
	UpdatedReplicas int32 `json:"updatedReplicas,omitempty"`

	// OperatingReplicas indicates the number of targets during target ops lifecycle and not finish update-phase.
	// +optional
	OperatingReplicas int32 `json:"operatingReplicas,omitempty"`

	// UpdatedReadyReplicas indicates the number of the target with updated revision and ready condition
	// +optional
	UpdatedReadyReplicas int32 `json:"updatedReadyReplicas,omitempty"`

	// The number of available replicas for this replica set.
	// +optional
	AvailableReplicas int32 `json:"availableReplicas,omitempty"`

	// UpdatedAvailableReplicas indicates the number of available updated revision replicas for this replicas set.
	// A target is updated available means the target is ready for updated revision and accessible
	// +optional
	UpdatedAvailableReplicas int32 `json:"updatedAvailableReplicas,omitempty"`

	// Represents the latest available observations of a XSet's current state.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// OpsPriority is used to store the ops priority of a target
type OpsPriority struct {
	// PriorityClass is the priority class of the target
	PriorityClass int32
	// DeletionCost is the deletion cost of the target
	DeletionCost int32
}
