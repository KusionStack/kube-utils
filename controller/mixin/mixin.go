/**
 * Copyright 2023 KusionStack Authors.
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
package mixin

import (
	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

// ReconcilerMixin is intended to simplify the construction of Reconciler
//
// Example:
//
//		type Reconciler struct {
//		  *mixin.ReconcilerMixin
//		}
//
//		func NewReconciler(mgr manager.Manager) reconcile.Reconciler {
//		  return &Reconciler{
//		    ReconcilerMixin: mixin.NewReconcilerMixin(name, mgr),
//	   }
type ReconcilerMixin struct {
	name string

	Scheme       *runtime.Scheme
	Config       *rest.Config
	Client       client.Client
	FieldIndexer client.FieldIndexer
	APIReader    client.Reader
	Cache        cache.Cache
	Logger       logr.Logger
	RESTMapper   meta.RESTMapper
	Recorder     record.EventRecorder
}

func NewReconcilerMixin(controllerName string, mgr manager.Manager) *ReconcilerMixin {
	// mgr.SetFields() is deprecated, It will be removed in v0.10.
	// So we should get fields directly.
	m := &ReconcilerMixin{
		name:         controllerName,
		Scheme:       mgr.GetScheme(),
		Config:       mgr.GetConfig(),
		Client:       mgr.GetClient(),
		FieldIndexer: mgr.GetFieldIndexer(),
		APIReader:    mgr.GetAPIReader(),
		Cache:        mgr.GetCache(),
		Logger:       mgr.GetLogger().WithName(controllerName),
		RESTMapper:   mgr.GetRESTMapper(),
		Recorder:     mgr.GetEventRecorderFor(controllerName),
	}
	return m
}

func (m *ReconcilerMixin) GetControllerName() string {
	return m.name
}
