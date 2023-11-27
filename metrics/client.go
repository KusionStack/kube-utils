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

package metrics

import (
	"context"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"

	"sigs.k8s.io/controller-runtime/pkg/client"
	k8smetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

const (
	ReconcileSubSystem = "reconcile"
	RestRequestCount   = "rest_request_count"
)

var (
	controllerRestRequestCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Subsystem: ReconcileSubSystem,
		Name:      RestRequestCount,
		Help:      "count the rest request send by controller",
	}, []string{"controller", "resource", "status", "method", "code"})
)

func init() {
	k8smetrics.Registry.MustRegister(controllerRestRequestCounter)
}

func NewControllerRestRequestCounter(controller, resource, status, method, code string) prometheus.Counter {
	return controllerRestRequestCounter.WithLabelValues(controller, resource, status, method, code)
}

func CodeForError(err error) int32 {
	switch t := err.(type) {
	case errors.APIStatus:
		return t.Status().Code
	}
	return 0
}

func Wrap(c client.Client, controller string) client.Client {
	return &ClientWithMetrics{c: c, controller: controller}
}

type ClientWithMetrics struct {
	c client.Client

	controller string
}

// Scheme returns the scheme this client is using.
func (cm *ClientWithMetrics) Scheme() *runtime.Scheme {
	return cm.c.Scheme()
}

// RESTMapper returns the rest this client is using.
func (cm *ClientWithMetrics) RESTMapper() meta.RESTMapper {
	return cm.c.RESTMapper()
}

func (cm *ClientWithMetrics) Get(ctx context.Context, key client.ObjectKey, obj client.Object) error {
	return cm.c.Get(ctx, key, obj)
}

func (cm *ClientWithMetrics) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	return cm.c.List(ctx, list, opts...)
}

// Create saves the object obj in the Kubernetes cluster.
func (cm *ClientWithMetrics) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	err := cm.c.Create(ctx, obj, opts...)
	if err == nil {
		NewControllerRestRequestCounter(cm.controller, obj.GetObjectKind().GroupVersionKind().Kind, "false", "POST", "200").Inc()
	} else {
		NewControllerRestRequestCounter(cm.controller, obj.GetObjectKind().GroupVersionKind().Kind, "false", "POST", fmt.Sprintf("%d", CodeForError(err))).Inc()
	}

	return err
}

// Delete deletes the given obj from Kubernetes cluster.
func (cm *ClientWithMetrics) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	err := cm.c.Delete(ctx, obj, opts...)
	if err == nil {
		NewControllerRestRequestCounter(cm.controller, obj.GetObjectKind().GroupVersionKind().Kind, "false", "DELETE", "200").Inc()
	} else {
		NewControllerRestRequestCounter(cm.controller, obj.GetObjectKind().GroupVersionKind().Kind, "false", "DELETE", fmt.Sprintf("%d", CodeForError(err))).Inc()
	}

	return err
}

// Update updates the given obj in the Kubernetes cluster. obj must be a
// struct pointer so that obj can be updated with the content returned by the Server.
func (cm *ClientWithMetrics) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	err := cm.c.Update(ctx, obj, opts...)
	if err == nil {
		NewControllerRestRequestCounter(cm.controller, obj.GetObjectKind().GroupVersionKind().Kind, "false", "PUT", "200").Inc()
	} else {
		NewControllerRestRequestCounter(cm.controller, obj.GetObjectKind().GroupVersionKind().Kind, "false", "PUT", fmt.Sprintf("%d", CodeForError(err))).Inc()
	}

	return err
}

// Patch patches the given obj in the Kubernetes cluster. obj must be a
// struct pointer so that obj can be updated with the content returned by the Server.
func (cm *ClientWithMetrics) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	err := cm.c.Patch(ctx, obj, patch, opts...)
	if err == nil {
		NewControllerRestRequestCounter(cm.controller, obj.GetObjectKind().GroupVersionKind().Kind, "false", "PATCH", "200").Inc()
	} else {
		NewControllerRestRequestCounter(cm.controller, obj.GetObjectKind().GroupVersionKind().Kind, "false", "PATCH", fmt.Sprintf("%d", CodeForError(err))).Inc()
	}

	return err
}

// DeleteAllOf deletes all objects of the given type matching the given options.
func (cm *ClientWithMetrics) DeleteAllOf(ctx context.Context, obj client.Object, opts ...client.DeleteAllOfOption) error {
	return cm.c.DeleteAllOf(ctx, obj, opts...)
}

func (cm *ClientWithMetrics) Status() client.StatusWriter {
	return &StatusClientWithMetrics{w: cm.c.Status(), controller: cm.controller}
}

type StatusClientWithMetrics struct {
	w client.StatusWriter

	controller string
}

// Update updates the fields corresponding to the status subresource for the
// given obj. obj must be a struct pointer so that obj can be updated
// with the content returned by the Server.
func (w *StatusClientWithMetrics) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	err := w.w.Update(ctx, obj, opts...)
	if err == nil {
		NewControllerRestRequestCounter(w.controller, obj.GetObjectKind().GroupVersionKind().Kind, "true", "PUT", "200").Inc()
	} else {
		NewControllerRestRequestCounter(w.controller, obj.GetObjectKind().GroupVersionKind().Kind, "true", "PUT", fmt.Sprintf("%d", CodeForError(err))).Inc()
	}

	return err
}

// Patch patches the given object's subresource. obj must be a struct
// pointer so that obj can be updated with the content returned by the
// Server.
func (w *StatusClientWithMetrics) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	err := w.w.Patch(ctx, obj, patch, opts...)
	if err == nil {
		NewControllerRestRequestCounter(w.controller, obj.GetObjectKind().GroupVersionKind().Kind, "true", "PATCH", "200").Inc()
	} else {
		NewControllerRestRequestCounter(w.controller, obj.GetObjectKind().GroupVersionKind().Kind, "true", "PATCH", fmt.Sprintf("%d", CodeForError(err))).Inc()
	}

	return err
}
