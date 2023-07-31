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

package handler

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/runtime/inject"
)

var _ handler.EventHandler = &EnqueueRequestsFromMapFuncWithRateLimit{}

// EnqueueRequestsFromMapFunc enqueues Requests by running a transformation function that outputs a collection
// of reconcile.Requests on each Event.  The reconcile.Requests may be for an arbitrary set of objects
// defined by some user specified transformation of the source Event.  (e.g. trigger Reconciler for a set of objects
// in response to a cluster resize event caused by adding or deleting a Node)
//
// EnqueueRequestsFromMapFunc is frequently used to fan-out updates from one object to one or more other
// objects of a differing type.
//
// For UpdateEvents which contain both a new and old object, the transformation function is run on both
// objects and both sets of Requests are enqueue.
type EnqueueRequestsFromMapFuncWithRateLimit struct {
	// Mapper transforms the argument into a slice of keys to be reconciled
	ToRequests handler.MapFunc
}

// Create implements EventHandler
func (e *EnqueueRequestsFromMapFuncWithRateLimit) Create(evt event.CreateEvent, q workqueue.RateLimitingInterface) {
	e.mapAndEnqueue(q, evt.Object)
}

// Update implements EventHandler
func (e *EnqueueRequestsFromMapFuncWithRateLimit) Update(evt event.UpdateEvent, q workqueue.RateLimitingInterface) {
	e.mapAndEnqueue(q, evt.ObjectOld)
	e.mapAndEnqueue(q, evt.ObjectNew)
}

// Delete implements EventHandler
func (e *EnqueueRequestsFromMapFuncWithRateLimit) Delete(evt event.DeleteEvent, q workqueue.RateLimitingInterface) {
	e.mapAndEnqueue(q, evt.Object)
}

// Generic implements EventHandler
func (e *EnqueueRequestsFromMapFuncWithRateLimit) Generic(evt event.GenericEvent, q workqueue.RateLimitingInterface) {
	e.mapAndEnqueue(q, evt.Object)
}

func (e *EnqueueRequestsFromMapFuncWithRateLimit) mapAndEnqueue(q workqueue.RateLimitingInterface, object client.Object) {
	for _, req := range e.ToRequests(object) {
		Enqueue(req, q)
	}
}

// EnqueueRequestsFromMapFunc can inject fields into the mapper.

// InjectFunc implements inject.Injector.
func (e *EnqueueRequestsFromMapFuncWithRateLimit) InjectFunc(f inject.Func) error {
	if f == nil {
		return nil
	}
	return f(e.ToRequests)
}

// MapObject contains information from an event to be transformed into a Request.
type MapObject struct {
	// Meta is the meta data for an object from an event.
	Meta metav1.Object

	// Object is the object from an event.
	Object runtime.Object
}
