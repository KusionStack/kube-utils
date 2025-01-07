/*
Copyright 2016 The Kubernetes Authors.
Copyright 2023 The KusionStack Authors.

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
	"sync"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
)

// ObjectControllerRefManager is an interface for controllers that want to claim
// objects.
type ObjectControllerRefManager interface {
	// Claim tries to take ownership of an object for this controller.
	//
	// It will reconcile the following:
	//   - Adopt orphans if the match function returns true.
	//   - Release owned objects if the match function returns false.
	//
	// A non-nil error is returned if some form of reconciliation was attempted and
	// failed. Usually, controllers should try again later in case reconciliation
	// is still needed.
	//
	// If the error is nil, either the reconciliation succeeded, or no
	// reconciliation was necessary. The returned boolean indicates whether you now
	// own the object.
	//
	// No reconciliation will be attempted if the controller is being deleted.
	Claim(ctx context.Context, child metav1.Object) (bool, error)
	// ClaimAllOf tries to take ownership of a list of object
	//
	// It will reconcile the following:
	//   - Adopt orphans if the selector matches.
	//   - Release owned objects if the selector no longer matches.
	//
	// A non-nil error is returned if some form of reconciliation was attempted and
	// failed. Usually, controllers should try again later in case reconciliation
	// is still needed.
	//
	// If the error is nil, either the reconciliation succeeded, or no
	// reconciliation was necessary.
	//
	// The list of Object that you now own is returned.
	ClaimAllOf(ctx context.Context, children []metav1.Object) ([]metav1.Object, error)
}

type MatchFunc func(metav1.Object) bool

func LabelSelectorAsMatch(selector *metav1.LabelSelector) (MatchFunc, error) {
	if selector == nil {
		return nil, fmt.Errorf("selector is nil")
	}
	labelSelector, err := metav1.LabelSelectorAsSelector(selector)
	if err != nil {
		return nil, err
	}
	return func(o metav1.Object) bool {
		return labelSelector.Matches(labels.Set(o.GetLabels()))
	}, nil
}

func MatchAll() MatchFunc {
	return func(o metav1.Object) bool {
		return true
	}
}

func NewObjectControllerRefManager(
	writer OwnerRefWriter,
	controller metav1.Object,
	gvk schema.GroupVersionKind,
	match MatchFunc,
) ObjectControllerRefManager {
	return &objectControllerRefManager{
		writer:         writer,
		match:          match,
		controllerObj:  controller,
		controllerKind: gvk,
		canAdoptFunc:   canAdopt,
	}
}

// objectControllerRefManager is an interface for controllers that want to claim
// objects.
type objectControllerRefManager struct {
	writer         OwnerRefWriter
	match          MatchFunc
	controllerObj  metav1.Object
	controllerKind schema.GroupVersionKind

	once         sync.Once
	canAdoptFunc func(metav1.Object) error
	canAdoptErr  error
}

// ClaimAllOf tries to take ownership of a list of object
//
// It will reconcile the following:
//   - Adopt orphans if the selector matches.
//   - Release owned objects if the selector no longer matches.
//
// A non-nil error is returned if some form of reconciliation was attempted and
// failed. Usually, controllers should try again later in case reconciliation
// is still needed.
//
// If the error is nil, either the reconciliation succeeded, or no
// reconciliation was necessary.
//
// The list of Object that you now own is returned.
func (m *objectControllerRefManager) ClaimAllOf(ctx context.Context, objs []metav1.Object) ([]metav1.Object, error) {
	var claimObjs []metav1.Object
	var errList []error
	for _, obj := range objs {
		ok, err := m.Claim(ctx, obj)
		if err != nil {
			errList = append(errList, err)
			continue
		}
		if ok {
			claimObjs = append(claimObjs, obj)
		}
	}
	return claimObjs, utilerrors.NewAggregate(errList)
}

// Claim tries to take ownership of an object for this controller.
//
// It will reconcile the following:
//   - Adopt orphans if the match function returns true.
//   - Release owned objects if the match function returns false.
//
// A non-nil error is returned if some form of reconciliation was attempted and
// failed. Usually, controllers should try again later in case reconciliation
// is still needed.
//
// If the error is nil, either the reconciliation succeeded, or no
// reconciliation was necessary. The returned boolean indicates whether you now
// own the object.
//
// No reconciliation will be attempted if the controller is being deleted.
func (m *objectControllerRefManager) Claim(ctx context.Context, child metav1.Object) (bool, error) {
	controllerRef := metav1.GetControllerOf(child)
	if controllerRef != nil {
		if controllerRef.UID != m.controllerObj.GetUID() {
			// Owned by someone else. Ignore.
			return false, nil
		}
		if m.match(child) {
			// We already own it and the selector matches.
			// Return true (successfully claimed) before checking deletion timestamp.
			// We're still allowed to claim things we already own while being deleted
			// because doing so requires taking no actions.
			return true, nil
		}
		// Owned by us but selector doesn't match.
		// Try to release, unless we're being deleted.
		if m.controllerObj.GetDeletionTimestamp() != nil {
			return false, nil
		}
		if err := m.writer.Release(ctx, m.controllerObj, child); err != nil {
			// If the pod no longer exists, ignore the error.
			if apierrors.IsNotFound(err) {
				return false, nil
			}
			// Either someone else released it, or there was a transient error.
			// The controller should requeue and try again if it's still stale.
			return false, err
		}
		// Successfully released.
		return false, nil
	}

	// It's an orphan.
	if m.controllerObj.GetDeletionTimestamp() != nil || !m.match(child) {
		// Ignore if we're being deleted or selector doesn't match.
		return false, nil
	}
	if child.GetDeletionTimestamp() != nil {
		// Ignore if the object is being deleted
		return false, nil
	}
	// Selector matches. Try to adopt.
	if err := m.adopt(ctx, child); err != nil {
		// If the obj no longer exists, ignore the error.
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		// Either someone else claimed it first, or there was a transient error.
		// The controller should requeue and try again if it's still orphaned.
		return false, err
	}
	// Successfully adopted.
	return true, nil
}

func (m *objectControllerRefManager) canAdoptOnce() error {
	m.once.Do(func() {
		m.canAdoptErr = m.canAdoptFunc(m.controllerObj)
	})

	return m.canAdoptErr
}

func (m *objectControllerRefManager) adopt(ctx context.Context, obj metav1.Object) error {
	if err := m.canAdoptOnce(); err != nil {
		return err
	}
	return m.writer.Adopt(ctx, m.controllerObj, m.controllerKind, obj)
}

func canAdopt(obj metav1.Object) error {
	if obj.GetDeletionTimestamp() != nil {
		return fmt.Errorf("can't adopt object because Owner %v/%v has just been deleted at %v",
			obj.GetNamespace(), obj.GetName(), obj.GetDeletionTimestamp())
	}
	return nil
}
