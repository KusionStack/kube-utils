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
	appsv1alpha1 "kusionstack.io/kube-api/apps/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type XSetLabelAnnotationEnum int

const EnumXSetLabelAnnotationsNum = int(wellKnownCount)
const (
	// OperatingLabelPrefix indicates a target is under operation
	// set by xset controller
	OperatingLabelPrefix XSetLabelAnnotationEnum = iota

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

	// ControlledByXSetLabel indicates a target is controlled by xset.
	// set by xset controller
	ControlledByXSetLabel

	// XInstanceIdLabelKey is used to attach instance ID on x
	XInstanceIdLabelKey

	// XSetUpdateIndicationLabelKey is used to indicate a target is updated by xset
	XSetUpdateIndicationLabelKey

	// XDeletionIndicationLabelKey is used to indicate a target is deleted by xset
	XDeletionIndicationLabelKey

	// XReplaceIndicationLabelKey is used to indicate a target is replaced by xset
	XReplaceIndicationLabelKey

	// XReplacePairNewId is used to indicate the new created target on replace origin target
	XReplacePairNewId

	// XReplacePairOriginName is used to indicate replace origin target name on the new created target
	XReplacePairOriginName

	// XReplaceByReplaceUpdateLabelKey indicates a target is replaced by update by xset
	XReplaceByReplaceUpdateLabelKey

	// XOrphanedIndicationLabelKey is used to indicate a target is orphaned by xset
	XOrphanedIndicationLabelKey

	// XCreatingLabel indicates a target is creating by xset
	XCreatingLabel

	// XCompletingLabel indicates a target is completing by xset
	XCompletingLabel

	// XExcludeIndicationLabelKey is used to indicate a target is excluded by xset
	XExcludeIndicationLabelKey

	// SubResourcePvcTemplateLabelKey is used to attach pvc template name to pvc resources
	SubResourcePvcTemplateLabelKey

	// SubResourcePvcTemplateHashLabelKey is used to attach hash of pvc template to pvc subresource
	SubResourcePvcTemplateHashLabelKey

	// LastXStatusAnnotationKey is used to record the last status of a target by xset
	LastXStatusAnnotationKey

	// wellKnownCount is the number of XSetLabelAnnotationEnum
	wellKnownCount
)

type XSetLabelAnnotationManager interface {
	Get(labels map[string]string, labelType XSetLabelAnnotationEnum) (string, bool)
	Set(obj client.Object, labelType XSetLabelAnnotationEnum, value string)
	Delete(labels map[string]string, labelType XSetLabelAnnotationEnum)
	Value(labelType XSetLabelAnnotationEnum) string
}

var defaultXSetLabelAnnotationManager = map[XSetLabelAnnotationEnum]string{
	OperatingLabelPrefix:         appsv1alpha1.PodOperatingLabelPrefix,
	OperationTypeLabelPrefix:     appsv1alpha1.PodOperationTypeLabelPrefix,
	OperateLabelPrefix:           appsv1alpha1.PodOperateLabelPrefix,
	UndoOperationTypeLabelPrefix: appsv1alpha1.PodUndoOperationTypeLabelPrefix,
	ServiceAvailableLabel:        appsv1alpha1.PodServiceAvailableLabel,
	PreparingDeleteLabel:         appsv1alpha1.PodPreparingDeleteLabel,

	ControlledByXSetLabel:              appsv1alpha1.ControlledByKusionStackLabelKey,
	XInstanceIdLabelKey:                appsv1alpha1.PodInstanceIDLabelKey,
	XSetUpdateIndicationLabelKey:       appsv1alpha1.CollaSetUpdateIndicateLabelKey,
	XDeletionIndicationLabelKey:        appsv1alpha1.PodDeletionIndicationLabelKey,
	XReplaceIndicationLabelKey:         appsv1alpha1.PodReplaceIndicationLabelKey,
	XReplacePairNewId:                  appsv1alpha1.PodReplacePairNewId,
	XReplacePairOriginName:             appsv1alpha1.PodReplacePairOriginName,
	XReplaceByReplaceUpdateLabelKey:    appsv1alpha1.PodReplaceByReplaceUpdateLabelKey,
	XOrphanedIndicationLabelKey:        appsv1alpha1.PodOrphanedIndicateLabelKey,
	XCreatingLabel:                     appsv1alpha1.PodCreatingLabel,
	XCompletingLabel:                   appsv1alpha1.PodCompletingLabel,
	XExcludeIndicationLabelKey:         appsv1alpha1.PodExcludeIndicationLabelKey,
	SubResourcePvcTemplateLabelKey:     appsv1alpha1.PvcTemplateLabelKey,
	SubResourcePvcTemplateHashLabelKey: appsv1alpha1.PvcTemplateHashLabelKey,

	LastXStatusAnnotationKey: appsv1alpha1.LastPodStatusAnnotationKey,
}

func NewXSetLabelAnnotationManager() XSetLabelAnnotationManager {
	return &xSetLabelAnnotationManager{
		labelManager: defaultXSetLabelAnnotationManager,
	}
}

type xSetLabelAnnotationManager struct {
	labelManager map[XSetLabelAnnotationEnum]string
}

func (m *xSetLabelAnnotationManager) Get(labels map[string]string, key XSetLabelAnnotationEnum) (string, bool) {
	if labels == nil {
		return "", false
	}
	labelKey := m.labelManager[key]
	val, exist := labels[labelKey]
	return val, exist
}

func (m *xSetLabelAnnotationManager) Set(obj client.Object, key XSetLabelAnnotationEnum, val string) {
	if obj.GetLabels() == nil {
		obj.SetLabels(map[string]string{})
	}
	labelKey := m.labelManager[key]
	obj.GetLabels()[labelKey] = val
}

func (m *xSetLabelAnnotationManager) Delete(labels map[string]string, key XSetLabelAnnotationEnum) {
	if labels == nil {
		return
	}
	labelKey := m.labelManager[key]
	delete(labels, labelKey)
}

func (m *xSetLabelAnnotationManager) Value(key XSetLabelAnnotationEnum) string {
	return m.labelManager[key]
}

func GetWellKnownLabelPrefixesWithID(m XSetLabelAnnotationManager) []string {
	return []string{
		m.Value(OperatingLabelPrefix),
		m.Value(OperationTypeLabelPrefix),
		m.Value(UndoOperationTypeLabelPrefix),
		m.Value(OperatingLabelPrefix),
	}
}

func GetXSetLabelAnnotationManager(xsetController XSetController) XSetLabelAnnotationManager {
	if getter, ok := xsetController.(LabelAnnotationManagerGetter); ok {
		return getter.GetLabelManagerAdapter()
	}
	return NewXSetLabelAnnotationManager()
}
