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

package history

import (
	"bytes"
	"context"
	"fmt"

	"github.com/go-logr/logr"
	"github.com/samber/lo"
	apps "k8s.io/api/apps/v1"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	clientutil "kusionstack.io/kube-utils/client"
	"kusionstack.io/kube-utils/controller/refmanager"
)

// RevisionControlInterface provides an interface allowing for management of a Controller's history as realized by recorded
// ControllerRevisions. An instance of RevisionControlInterface can be retrieved from NewHistory. Implementations must treat all
// pointer parameters as "in" parameter, and they must not be mutated.
type RevisionControlInterface interface {
	// ListControllerRevisions lists all ControllerRevisions matching selector and owned by parent or no other
	// controller. If the returned error is nil the returned slice of ControllerRevisions is valid. If the
	// returned error is not nil, the returned slice is not valid.
	ListControllerRevisions(ctx context.Context, parent metav1.Object, selector labels.Selector) ([]*appsv1.ControllerRevision, error)
	// CreateControllerRevision attempts to create the revision as owned by parent via a ControllerRef. If name
	// collision occurs, collisionCount (incremented each time collision occurs except for the first time) is
	// added to the hash of the revision and it is renamed using ControllerRevisionName. Implementations may
	// cease to attempt to retry creation after some number of attempts and return an error. If the returned
	// error is not nil, creation failed. If the returned error is nil, the returned ControllerRevision has been
	// created.
	// Callers must make sure that collisionCount is not nil. An error is returned if it is.
	CreateControllerRevision(ctx context.Context, parent metav1.Object, revision *apps.ControllerRevision, collisionCount *int32) (*apps.ControllerRevision, error)
	// DeleteControllerRevision attempts to delete revision. If the returned error is not nil, deletion has failed.
	DeleteControllerRevision(ctx context.Context, revision *apps.ControllerRevision) error
	// UpdateControllerRevision updates revision such that its Revision is equal to newRevision. Implementations
	// may retry on conflict. If the returned error is nil, the update was successful and returned ControllerRevision
	// is valid. If the returned error is not nil, the update failed and the returned ControllerRevision is invalid.
	UpdateControllerRevision(ctx context.Context, revision *apps.ControllerRevision, newRevision int64) (*apps.ControllerRevision, error)
	// AdoptControllerRevision attempts to adopt revision by adding a ControllerRef indicating that the parent
	// Object of parentKind is the owner of revision. If revision is already owned, an error is returned. If the
	// resource patch fails, an error is returned. If no error is returned, the returned ControllerRevision is
	// valid.
	AdoptControllerRevision(ctx context.Context, parent metav1.Object, parentKind schema.GroupVersionKind, revision *apps.ControllerRevision) (*apps.ControllerRevision, error)
	// ReleaseControllerRevision attempts to release parent's ownership of revision by removing parent from the
	// OwnerReferences of revision. If an error is returned, parent remains the owner of revision. If no error is
	// returned, the returned ControllerRevision is valid.
	ReleaseControllerRevision(ctx context.Context, parent metav1.Object, revision *apps.ControllerRevision) (*apps.ControllerRevision, error)
}

var _ RevisionControlInterface = &realRevisionControl{}

type realRevisionControl struct {
	reader client.Reader
	writer client.Writer
}

func NewRevisionControl(reader client.Reader, writer client.Writer) RevisionControlInterface {
	return &realRevisionControl{
		reader: reader,
		writer: writer,
	}
}

func (h *realRevisionControl) ListControllerRevisions(ctx context.Context, parent metav1.Object, selector labels.Selector) ([]*appsv1.ControllerRevision, error) {
	list := &appsv1.ControllerRevisionList{}
	err := h.reader.List(ctx, list, client.InNamespace(parent.GetNamespace()), client.MatchingLabelsSelector{Selector: selector})
	if err != nil {
		return nil, err
	}

	result := lo.FilterMap(list.Items, func(item appsv1.ControllerRevision, _ int) (*appsv1.ControllerRevision, bool) {
		owner := metav1.GetControllerOfNoCopy(&item)
		if owner != nil && owner.UID != parent.GetUID() {
			return nil, false
		}
		// return orphan or controlled revisions
		return item.DeepCopy(), true
	})

	return result, nil
}

func (h *realRevisionControl) CreateControllerRevision(ctx context.Context, parent metav1.Object, revision *apps.ControllerRevision, collisionCount *int32) (*apps.ControllerRevision, error) {
	if collisionCount == nil {
		return nil, fmt.Errorf("collisionCount should not be nil")
	}

	logger := logr.FromContextOrDiscard(ctx)

	// Clone the input
	clone := revision.DeepCopy()

	if ns := parent.GetNamespace(); len(ns) > 0 {
		clone.Namespace = ns
	}

	originalCollisionCount := *collisionCount
	var err error
	// Continue to attempt to create the revision updating the name with a new hash on each iteration
	for {
		hash := HashControllerRevision(revision, collisionCount)
		// Update the revisions name
		clone.Name = ControllerRevisionName(parent.GetName(), hash)
		err = h.writer.Create(ctx, clone)
		if errors.IsAlreadyExists(err) {
			exists := &apps.ControllerRevision{}
			err := h.reader.Get(ctx, types.NamespacedName{Namespace: clone.Namespace, Name: clone.Name}, exists)
			if err != nil {
				return nil, err
			}
			existingOwner := metav1.GetControllerOfNoCopy(exists)
			// existing revision hash a owner refer to parent, and data bytes is equal to current revision data bytes
			// then return exists revision
			if existingOwner != nil && existingOwner.UID == parent.GetUID() &&
				bytes.Equal(exists.Data.Raw, clone.Data.Raw) {
				return exists, nil
			}
			*collisionCount++
			continue
		}

		logger.V(3).Info("controller revision created", "namespace", clone.Namespace, "name", clone.Name, "originalCollision", originalCollisionCount, "collisionCount", *collisionCount)
		return clone, err
	}
}

// UpdateControllerRevision implements History.
func (h *realRevisionControl) UpdateControllerRevision(ctx context.Context, revision *appsv1.ControllerRevision, newRevision int64) (*appsv1.ControllerRevision, error) {
	clone := revision.DeepCopy()
	oldRevision := clone.Revision
	changed, err := clientutil.UpdateOnConflict(ctx, h.reader, h.writer, clone, func(obj *apps.ControllerRevision) error {
		oldRevision = obj.Revision
		obj.Revision = newRevision
		return nil
	})
	if changed {
		logger := logr.FromContextOrDiscard(ctx)
		logger.V(3).Info("controller revision changed", "namespace", clone.Namespace, "name", clone.Name, "from", oldRevision, "to", newRevision)
	}
	return clone, err
}

func (h *realRevisionControl) DeleteControllerRevision(ctx context.Context, revision *apps.ControllerRevision) error {
	err := h.writer.Delete(ctx, revision)
	if err == nil {
		logger := logr.FromContextOrDiscard(ctx)
		logger.V(3).Info("controller revision deleted", "namespace", revision.Namespace, "name", revision.Name)
		return nil
	}
	return client.IgnoreNotFound(err)
}

func (h *realRevisionControl) AdoptControllerRevision(ctx context.Context, parent metav1.Object, parentKind schema.GroupVersionKind, revision *apps.ControllerRevision) (*apps.ControllerRevision, error) {
	oldOwner := metav1.GetControllerOfNoCopy(revision)
	newOwner := metav1.NewControllerRef(parent, parentKind)

	if oldOwner != nil {
		if oldOwner.UID != newOwner.UID {
			return revision, &controllerutil.AlreadyOwnedError{
				Object: revision,
				Owner:  *oldOwner,
			}
		}
		// no change
		return revision, nil
	}

	// orphan
	w := refmanager.NewOwnerRefWriter(h.writer)
	err := w.Adopt(ctx, parent, parentKind, revision)
	return revision, err
}

func (h *realRevisionControl) ReleaseControllerRevision(ctx context.Context, parent metav1.Object, revision *apps.ControllerRevision) (*apps.ControllerRevision, error) {
	w := refmanager.NewOwnerRefWriter(h.writer)
	err := w.Release(ctx, parent, revision)
	return revision, err
}
