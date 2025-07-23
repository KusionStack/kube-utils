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

import (
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type OperationLabelEnum int

const (
	// OperatingLabelPrefix indicates a target is under operation
	// set by xset controller
	OperatingLabelPrefix OperationLabelEnum = iota

	// OperationTypeLabelPrefix indicates the type of operation
	// set by xset controller
	OperationTypeLabelPrefix

	// OperateLabelPrefix indicates a target could start operation
	// set by related opsLifecycle controller.
	// xset controller will start operation only after this label is set
	OperateLabelPrefix

	// UndoOperationTypeLabelPrefix indicates a type of operation has been canceled.
	// need to be handled by related opsLifecycle controller
	UndoOperationTypeLabelPrefix

	// ServiceAvailableLabel indicates a target is available for service.
	// set by related opsLifecycle controller.
	ServiceAvailableLabel

	// PreparingDeleteLabel indicates a target is preparing to be deleted.
	// set by xset controller,
	// handle by related opsLifecycle controller if needed.
	PreparingDeleteLabel
)

type LifeCycleLabelManager interface {
	Get(labelType OperationLabelEnum) string
}

type OperationType string

var (
	OpsLifecycleTypeUpdate  OperationType = "update"
	OpsLifecycleTypeScaleIn OperationType = "scale-in"
	OpsLifecycleTypeDelete  OperationType = "delete"
)

// LifecycleAdapter helps CRD Operators to easily access TargetOpsLifecycle
type LifecycleAdapter interface {
	// GetID indicates ID of one TargetOpsLifecycle
	GetID() string

	// GetType indicates type for an Operator
	GetType() OperationType

	// AllowMultiType indicates whether multiple IDs which have the same Type are allowed
	AllowMultiType() bool

	// WhenBegin will be executed when begin a lifecycle
	WhenBegin(target client.Object) (needUpdate bool, err error)

	// WhenFinish will be executed when finish a lifecycle
	WhenFinish(target client.Object) (needUpdate bool, err error)
}
