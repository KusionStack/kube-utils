/*
Copyright 2025 The KusionStack Authors.

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

package refmanager

import (
	"context"
	"fmt"

	"github.com/samber/lo"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

type OwnerRefWriter interface {
	Adopt(ctx context.Context, parent metav1.Object, parentGVK schema.GroupVersionKind, child metav1.Object) error
	Release(ctx context.Context, parant, child metav1.Object) error
}

func NewOwnerRefWriter(client client.Writer) OwnerRefWriter {
	return &ownerRefWriter{
		client: client,
	}
}

type ownerRefWriter struct {
	client client.Writer
}

func (w *ownerRefWriter) Adopt(ctx context.Context, parent metav1.Object, parentGVK schema.GroupVersionKind, child metav1.Object) error {
	newOwner := metav1.NewControllerRef(parent, parentGVK)
	if existing := metav1.GetControllerOfNoCopy(child); existing != nil && !ReferSameObject(*existing, *newOwner) {
		// conflict
		return &controllerutil.AlreadyOwnedError{
			Object: child,
			Owner:  *existing,
		}
	}

	upsertOwner(child, *newOwner)
	clientObj, ok := parent.(client.Object)
	if !ok {
		return fmt.Errorf("failed to convert parent object to controller-runtime client.Object")
	}
	return w.client.Update(ctx, clientObj)
}

func (w *ownerRefWriter) Release(ctx context.Context, parent, obj metav1.Object) error {
	oldOwners := obj.GetOwnerReferences()

	// filter out the owner that points to the parent
	newOwners := lo.Reject(oldOwners, func(ref metav1.OwnerReference, _ int) bool {
		return parent.GetUID() == ref.UID
	})

	// owners not changed
	if len(newOwners) == len(oldOwners) {
		return nil
	}

	obj.SetOwnerReferences(newOwners)
	clientObj, ok := obj.(client.Object)
	if !ok {
		return fmt.Errorf("failed to convert parent object to controller-runtime client.Object")
	}
	return w.client.Update(ctx, clientObj)
}

// Returns true if a and b point to the same object.
func ReferSameObject(a, b metav1.OwnerReference) bool {
	aGV, err := schema.ParseGroupVersion(a.APIVersion)
	if err != nil {
		return false
	}

	bGV, err := schema.ParseGroupVersion(b.APIVersion)
	if err != nil {
		return false
	}

	return aGV.Group == bGV.Group && a.Kind == b.Kind && a.Name == b.Name
}

func upsertOwner(obj metav1.Object, owner metav1.OwnerReference) {
	owners := obj.GetOwnerReferences()
	_, index, found := lo.FindIndexOf(obj.GetOwnerReferences(), func(ref metav1.OwnerReference) bool {
		return ReferSameObject(ref, owner)
	})
	if found {
		owners[index] = owner
	} else {
		owners = append(owners, owner)
	}
	obj.SetOwnerReferences(owners)
}
