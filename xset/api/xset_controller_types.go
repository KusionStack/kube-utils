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
	"context"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type XSetController interface {
	ControllerName() string
	FinalizerName() string

	XSetMeta() metav1.TypeMeta // todo gvk
	XMeta() metav1.TypeMeta
	NewXSetObject() XSetObject
	NewXObject() client.Object
	NewXObjectList() client.ObjectList

	// XSetOperation are implemented to access XSetSpec, XSetStatus, etc.
	XSetOperation
	// XOperation are implemented to access X object and status, etc.
	XOperation
}

type XSetObject client.Object

type XSetOperation interface {
	GetXSetSpec(object XSetObject) *XSetSpec
	GetXSetPatch(object metav1.Object) ([]byte, error)
	GetXSetStatus(object XSetObject) *XSetStatus
	SetXSetStatus(object XSetObject, status *XSetStatus)
	UpdateScaleStrategy(ctx context.Context, c client.Client, object XSetObject, scaleStrategy *ScaleStrategy) error
	GetXSetTemplatePatcher(object metav1.Object) func(client.Object) error
}

type XOperation interface {
	GetXObjectFromRevision(revision *appsv1.ControllerRevision) (client.Object, error)
	CheckScheduled(object client.Object) bool
	CheckReadyTime(object client.Object) (bool, *metav1.Time)
	CheckAvailable(object client.Object) bool
	CheckInactive(object client.Object) bool
	GetXOpsPriority(ctx context.Context, c client.Client, object client.Object) (*OpsPriority, error)
}

type SubResourcePvcAdapter interface {
	RetainPvcWhenXSetDeleted(object XSetObject) bool
	RetainPvcWhenXSetScaled(object XSetObject) bool
	GetXSetPvcTemplate(object XSetObject) []corev1.PersistentVolumeClaim
	GetXMountedPvcs(object client.Object) []corev1.Volume
	MountXPvcs(object client.Object, pvcs []corev1.Volume)
}

// LifecycleAdapterGetter is used to get lifecycle adapters.
type LifecycleAdapterGetter interface {
	GetScaleInOpsLifecycleAdapter() LifecycleAdapter
	GetUpdateOpsLifecycleAdapter() LifecycleAdapter
}

// ResourceContextAdapterGetter is used to get resource context adapter.
type ResourceContextAdapterGetter interface {
	GetResourceContextAdapter() ResourceContextAdapter
}

// LabelAnnotationManagerGetter is used to get label manager adapter.
type LabelAnnotationManagerGetter interface {
	GetLabelManagerAdapter() XSetLabelAnnotationManager
}
