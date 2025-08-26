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
	appsv1alpha1 "kusionstack.io/kube-api/apps/v1alpha1"

	"kusionstack.io/kube-utils/xset/api"
)

var defaultXSetControllerLabelManager = map[api.XSetControllerLabelEnum]string{
	api.EnumXSetControlledLabel:               appsv1alpha1.ControlledByKusionStackLabelKey,
	api.EnumXSetInstanceIdLabel:               appsv1alpha1.PodInstanceIDLabelKey,
	api.EnumXSetUpdateIndicationLabel:         appsv1alpha1.CollaSetUpdateIndicateLabelKey,
	api.EnumXSetDeletionIndicationLabel:       appsv1alpha1.PodDeletionIndicationLabelKey,
	api.EnumXSetReplaceIndicationLabel:        appsv1alpha1.PodReplaceIndicationLabelKey,
	api.EnumXSetReplacePairNewIdLabel:         appsv1alpha1.PodReplacePairNewId,
	api.EnumXSetReplacePairOriginNameLabel:    appsv1alpha1.PodReplacePairOriginName,
	api.EnumXSetReplaceByReplaceUpdateLabel:   appsv1alpha1.PodReplaceByReplaceUpdateLabelKey,
	api.EnumXSetOrphanedLabel:                 appsv1alpha1.PodOrphanedIndicateLabelKey,
	api.EnumXSetTargetCreatingLabel:           appsv1alpha1.PodCreatingLabel,
	api.EnumXSetTargetExcludeIndicationLabel:  appsv1alpha1.PodExcludeIndicationLabelKey,
	api.EnumXSetLastTargetStatusAnnotationKey: appsv1alpha1.LastPodStatusAnnotationKey,
}

func NewXSetControllerLabelManager() api.XSetLabelManager {
	return &xSetControllerLabelManager{
		labelManager: defaultXSetControllerLabelManager,
	}
}

type xSetControllerLabelManager struct {
	labelManager map[api.XSetControllerLabelEnum]string
}

func (m *xSetControllerLabelManager) Get(labels map[string]string, key api.XSetControllerLabelEnum) (string, bool) {
	if labels == nil {
		return "", false
	}
	labelKey := m.labelManager[key]
	val, exist := labels[labelKey]
	return val, exist
}

func (m *xSetControllerLabelManager) Set(labels map[string]string, key api.XSetControllerLabelEnum, val string) {
	if labels == nil {
		labels = make(map[string]string)
	}
	labelKey := m.labelManager[key]
	labels[labelKey] = val
}

func (m *xSetControllerLabelManager) Delete(labels map[string]string, key api.XSetControllerLabelEnum) {
	if labels == nil {
		return
	}
	labelKey := m.labelManager[key]
	delete(labels, labelKey)
}

func (m *xSetControllerLabelManager) Label(key api.XSetControllerLabelEnum) string {
	return m.labelManager[key]
}
