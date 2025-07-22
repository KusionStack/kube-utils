/*
 * Copyright 2024 - 2025 KusionStack Authors.
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
	"kusionstack.io/kube-utils/xset/api"
)

// OpsLifecycle default labels
var (
	defaultOperatingLabelPrefix         = "operating.opslifecycle.kusionstack.io"
	defaultOperationTypeLabelPrefix     = "operation-type.opslifecycle.kusionstack.io"
	defaultOperateLabelPrefix           = "operate.opslifecycle.kusionstack.io"
	defaultUndoOperationTypeLabelPrefix = "undo-operation-type.opslifecycle.kusionstack.io"
	defaultServiceAvailableLabel        = "opslifecycle.kusionstack.io/service-available"
	defaultPreparingDeleteLabel         = "opslifecycle.kusionstack.io/preparing-to-delete"
)

var defaultLables = map[api.OperationLabelEnum]string{
	api.OperatingLabelPrefix:         defaultOperatingLabelPrefix,
	api.OperationTypeLabelPrefix:     defaultOperationTypeLabelPrefix,
	api.OperateLabelPrefix:           defaultOperateLabelPrefix,
	api.UndoOperationTypeLabelPrefix: defaultUndoOperationTypeLabelPrefix,
	api.ServiceAvailableLabel:        defaultServiceAvailableLabel,
	api.PreparingDeleteLabel:         defaultPreparingDeleteLabel,
}

type LabelManagerImpl struct {
	labels                       map[api.OperationLabelEnum]string
	wellKnownLabelPrefixesWithID []string
}

func NewLabelManager(overwrite map[api.OperationLabelEnum]string) api.LifeCycleLabelManager {
	labelKeys := make(map[api.OperationLabelEnum]string)
	for k, v := range defaultLables {
		labelKeys[k] = v
	}
	if len(overwrite) > 0 {
		for k, v := range overwrite {
			labelKeys[k] = v
		}
	}

	wellKnownLabelPrefilxesWithID := []string{
		labelKeys[api.OperatingLabelPrefix],
		labelKeys[api.OperationTypeLabelPrefix],
		labelKeys[api.UndoOperationTypeLabelPrefix],
		labelKeys[api.OperatingLabelPrefix],
	}
	return &LabelManagerImpl{
		labels:                       labelKeys,
		wellKnownLabelPrefixesWithID: wellKnownLabelPrefilxesWithID,
	}
}

func (m *LabelManagerImpl) Get(labelType api.OperationLabelEnum) string {
	return m.labels[labelType]
}
