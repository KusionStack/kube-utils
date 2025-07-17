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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type XSetController interface {
	ControllerName() string
	FinalizerName() string
	XSetMeta() metav1.TypeMeta
	XMeta() metav1.TypeMeta

	EmptyXSetObject() XSetObject
	EmptyXObject() client.Object
	EmptyXObjectList() client.ObjectList

	GetXSetSpec(object XSetObject) *XSetSpec
	UpdateScaleStrategy(object XSetObject, scaleStrategy *ScaleStrategy) (err error)
	GetXSetStatus(object XSetObject) *XSetStatus
	SetXSetStatus(object XSetObject, status *XSetStatus)

	GetScaleInOpsLifecycleAdapter() LifecycleAdapter
	GetUpdateOpsLifecycleAdapter() LifecycleAdapter

	GetXSetPatch(object metav1.Object) ([]byte, error)
	GetXTemplate(object XSetObject) client.Object
	CheckReady(object client.Object) bool
}

type XSetObject client.Object
