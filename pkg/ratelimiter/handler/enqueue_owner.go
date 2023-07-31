/*
Copyright 2018 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package handler

import (
	"fmt"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/runtime/inject"
)

var _ handler.EventHandler = &EnqueueRequestForOwnerWithRateLimit{}

type EnqueueRequestForOwnerWithRateLimit struct {
	// OwnerType is the type of the Owner object to look for in OwnerReferences.  Only Group and Kind are compared.
	OwnerType runtime.Object

	// IsController if set will only look at the first OwnerReference with Controller: true.
	IsController bool

	// groupKind is the cached Group and Kind from OwnerType
	groupKind schema.GroupKind

	// mapper maps GroupVersionKinds to Resources
	mapper meta.RESTMapper
}

// Create implements EventHandler
func (e *EnqueueRequestForOwnerWithRateLimit) Create(evt event.CreateEvent, q workqueue.RateLimitingInterface) {
	for _, req := range e.getOwnerReconcileRequest(evt.Object) {
		Enqueue(req, q)
	}
}

// Update implements EventHandler
func (e *EnqueueRequestForOwnerWithRateLimit) Update(evt event.UpdateEvent, q workqueue.RateLimitingInterface) {
	for _, req := range e.getOwnerReconcileRequest(evt.ObjectOld) {
		Enqueue(req, q)
	}
	for _, req := range e.getOwnerReconcileRequest(evt.ObjectNew) {
		Enqueue(req, q)
	}
}

// Delete implements EventHandler
func (e *EnqueueRequestForOwnerWithRateLimit) Delete(evt event.DeleteEvent, q workqueue.RateLimitingInterface) {
	for _, req := range e.getOwnerReconcileRequest(evt.Object) {
		Enqueue(req, q)
	}
}

// Generic implements EventHandler
func (e *EnqueueRequestForOwnerWithRateLimit) Generic(evt event.GenericEvent, q workqueue.RateLimitingInterface) {
	for _, req := range e.getOwnerReconcileRequest(evt.Object) {
		Enqueue(req, q)
	}
}

// parseOwnerTypeGroupKind parses the OwnerType into a Group and Kind and caches the result.  Returns false
// if the OwnerType could not be parsed using the scheme.
func (e *EnqueueRequestForOwnerWithRateLimit) parseOwnerTypeGroupKind(scheme *runtime.Scheme) error {
	// Get the kinds of the type
	kinds, _, err := scheme.ObjectKinds(e.OwnerType)
	if err != nil {
		return err
	}
	// Expect only 1 kind.  If there is more than one kind this is probably an edge case such as ListOptions.
	if len(kinds) != 1 {
		err := fmt.Errorf("Expected exactly 1 kind for OwnerType %T, but found %s kinds", e.OwnerType, kinds)
		return err

	}
	// Cache the Group and Kind for the OwnerType
	e.groupKind = schema.GroupKind{Group: kinds[0].Group, Kind: kinds[0].Kind}
	return nil
}

// getOwnerReconcileRequest looks at object and returns a slice of reconcile.Request to reconcile
// owners of object that match e.OwnerType.
func (e *EnqueueRequestForOwnerWithRateLimit) getOwnerReconcileRequest(object metav1.Object) []reconcile.Request {
	// Iterate through the OwnerReferences looking for a match on Group and Kind against what was requested
	// by the user
	var result []reconcile.Request
	for _, ref := range e.getOwnersReferences(object) {
		// Parse the Group out of the OwnerReference to compare it to what was parsed out of the requested OwnerType
		refGV, err := schema.ParseGroupVersion(ref.APIVersion)
		if err != nil {
			return nil
		}

		// Compare the OwnerReference Group and Kind against the OwnerType Group and Kind specified by the user.
		// If the two match, create a Request for the objected referred to by
		// the OwnerReference.  Use the Name from the OwnerReference and the Namespace from the
		// object in the event.
		if ref.Kind == e.groupKind.Kind && refGV.Group == e.groupKind.Group {
			// Match found - add a Request for the object referred to in the OwnerReference
			request := reconcile.Request{NamespacedName: types.NamespacedName{
				Name: ref.Name,
			}}

			// if owner is not namespaced then we should set the namespace to the empty
			mapping, err := e.mapper.RESTMapping(e.groupKind, refGV.Version)
			if err != nil {
				return nil
			}
			if mapping.Scope.Name() != meta.RESTScopeNameRoot {
				request.Namespace = object.GetNamespace()
			}

			result = append(result, request)
		}
	}

	// Return the matches
	return result
}

// getOwnersReferences returns the OwnerReferences for an object as specified by the EnqueueRequestForOwner
// - if IsController is true: only take the Controller OwnerReference (if found)
// - if IsController is false: take all OwnerReferences
func (e *EnqueueRequestForOwnerWithRateLimit) getOwnersReferences(object metav1.Object) []metav1.OwnerReference {
	if object == nil {
		return nil
	}

	// If not filtered as Controller only, then use all the OwnerReferences
	if !e.IsController {
		return object.GetOwnerReferences()
	}
	// If filtered to a Controller, only take the Controller OwnerReference
	if ownerRef := metav1.GetControllerOf(object); ownerRef != nil {
		return []metav1.OwnerReference{*ownerRef}
	}
	// No Controller OwnerReference found
	return nil
}

var _ inject.Scheme = &EnqueueRequestForOwnerWithRateLimit{}

// InjectScheme is called by the Controller to provide a singleton scheme to the EnqueueRequestForOwner.
func (e *EnqueueRequestForOwnerWithRateLimit) InjectScheme(s *runtime.Scheme) error {
	return e.parseOwnerTypeGroupKind(s)
}

var _ inject.Mapper = &EnqueueRequestForOwnerWithRateLimit{}

// InjectMapper  is called by the Controller to provide the rest mapper used by the manager.
func (e *EnqueueRequestForOwnerWithRateLimit) InjectMapper(m meta.RESTMapper) error {
	e.mapper = m
	return nil
}
