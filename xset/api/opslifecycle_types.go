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
