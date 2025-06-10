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

import (
	"sigs.k8s.io/controller-runtime/pkg/client"

	"kusionstack.io/kube-utils/xset/api"
)

var _ api.LifecycleAdapter = &DefaultUpdateLifecycleAdapter{}

type DefaultUpdateLifecycleAdapter struct{}

func (d *DefaultUpdateLifecycleAdapter) GetID() string {
	return "xset"
}

func (d *DefaultUpdateLifecycleAdapter) GetType() api.OperationType {
	return api.OpsLifecycleTypeUpdate
}

func (d *DefaultUpdateLifecycleAdapter) AllowMultiType() bool {
	return true
}

func (d *DefaultUpdateLifecycleAdapter) WhenBegin(target client.Object) (bool, error) {
	setOperate(d, target)
	return true, nil
}

func (d *DefaultUpdateLifecycleAdapter) WhenFinish(target client.Object) (bool, error) {
	return false, nil
}

var _ api.LifecycleAdapter = &DefaultScaleInLifecycleAdapter{}

type DefaultScaleInLifecycleAdapter struct{}

func (d *DefaultScaleInLifecycleAdapter) GetID() string {
	return "xset"
}

func (d *DefaultScaleInLifecycleAdapter) GetType() api.OperationType {
	return api.OpsLifecycleTypeScaleIn
}

func (d *DefaultScaleInLifecycleAdapter) AllowMultiType() bool {
	return true
}

func (d *DefaultScaleInLifecycleAdapter) WhenBegin(target client.Object) (bool, error) {
	setOperate(d, target)
	return true, nil
}

func (d *DefaultScaleInLifecycleAdapter) WhenFinish(target client.Object) (bool, error) {
	return false, nil
}
