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
	"context"

	apps "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/utils/ptr"
)

const (
	// remainExtraUnusedRevisions is the number of extra unused revisions we
	// reserve for diagnostic purposes.
	remainExtraUnusedRevisions = 2
)

// HistoryManager is an interface that can be used to manage ControllerRevisions for a Controller.
type HistoryManager interface {
	// ConstructRevisions returns the current and update ControllerRevisions for parent object. It also
	// returns a collision count that records the number of name collisions set saw when creating
	// new ControllerRevisions. This count is incremented on every name collision and is used in
	// building the ControllerRevision names for name collision avoidance. This method may create
	// a new revision, or modify the Revision of an existing revision if an update to set is detected.
	// This method expects that revisions is sorted when supplied.
	ConstructRevisions(ctx context.Context, parent metav1.Object) (
		currentRevision *apps.ControllerRevision,
		updatedRevision *apps.ControllerRevision,
		revisions []*apps.ControllerRevision,
		collisionCount int32,
		createNewRevision bool,
		err error,
	)
}

// RevisionOwner is an interface that can be used to get the labels, collision count, and
// revision limit of a parent object.
type RevisionOwner interface {
	GetGroupVersionKind() schema.GroupVersionKind
	GetMatchLabels(parent metav1.Object) map[string]string
	GetCollisionCount(parent metav1.Object) *int32
	GetHistoryLimit(parent metav1.Object) int32
	GetPatch(parent metav1.Object) ([]byte, error)
	GetCurrentRevision(parent metav1.Object) string
	GetInUsedRevisions(parent metav1.Object) (sets.String, error)
}

func NewHistoryManager(history RevisionControlInterface, revisionOwner RevisionOwner) HistoryManager {
	return &historyManager{
		client:        history,
		revisionOwner: revisionOwner,
	}
}

type historyManager struct {
	client        RevisionControlInterface
	revisionOwner RevisionOwner
}

// ConstructRevisions returns the current and update ControllerRevisions for obj. It also
// returns a collision count that records the number of name collisions set saw when creating
// new ControllerRevisions. This count is incremented on every name collision and is used in
// building the ControllerRevision names for name collision avoidance. This method may create
// a new revision, or modify the Revision of an existing revision if an update to set is detected.
// This method expects that revisions is sorted when supplied.
func (h *historyManager) ConstructRevisions(ctx context.Context, obj metav1.Object) (
	currentRevision *apps.ControllerRevision,
	updatedRevision *apps.ControllerRevision,
	revisions []*apps.ControllerRevision,
	collisionCount int32,
	created bool,
	err error,
) {
	matchLabels := h.revisionOwner.GetMatchLabels(obj)
	labelSelector := &metav1.LabelSelector{
		MatchLabels: matchLabels,
	}
	// Use a local copy of CollisionCount from object to avoid modifying it directly.
	// This copy is returned so the value gets carried over to obj.Status in update.
	collisionCount = ptr.Deref(h.revisionOwner.GetCollisionCount(obj), 0)

	revisions, err = h.controlledHistories(ctx, obj, labelSelector)
	if err != nil {
		return currentRevision, updatedRevision, nil, collisionCount, false, err
	}

	SortControllerRevisions(revisions)
	if cleanedRevision, err := h.cleanExpiredRevision(ctx, obj, &revisions); err != nil {
		return currentRevision, updatedRevision, nil, collisionCount, false, err
	} else {
		revisions = *cleanedRevision
	}

	// create a new revision from the current set
	updatedRevision, err = h.newRevision(obj, nextRevision(revisions), &collisionCount)
	if err != nil {
		return nil, nil, nil, collisionCount, false, err
	}

	// find any equivalent revisions
	equalRevisions := FindEqualRevisions(revisions, updatedRevision)
	equalCount := len(equalRevisions)
	revisionCount := len(revisions)

	created = false
	if equalCount > 0 && EqualRevision(revisions[revisionCount-1], equalRevisions[equalCount-1]) {
		// if the equivalent revision is immediately prior the update revision has not changed
		updatedRevision = revisions[revisionCount-1]
	} else if equalCount > 0 {
		// if the equivalent revision is not immediately prior we will roll back by incrementing the
		// Revision of the equivalent revision
		updatedRevision, err = h.client.UpdateControllerRevision(ctx, equalRevisions[equalCount-1], updatedRevision.Revision)
		if err != nil {
			return nil, nil, nil, collisionCount, false, err
		}
	} else {
		// if there is no equivalent revision we create a new one
		updatedRevision, err = h.client.CreateControllerRevision(ctx, obj, updatedRevision, &collisionCount)
		if err != nil {
			return nil, nil, nil, collisionCount, false, err
		}

		revisions = append(revisions, updatedRevision)
		created = true
	}

	// attempt to find the revision that corresponds to the current revision
	for i := range revisions {
		if revisions[i].Name == h.revisionOwner.GetCurrentRevision(obj) {
			currentRevision = revisions[i]
			break
		}
	}

	// if the current revision is nil we initialize the history by setting it to the update revision
	if currentRevision == nil {
		currentRevision = updatedRevision
	}

	return currentRevision, updatedRevision, revisions, collisionCount, created, nil
}

// controlledHistories returns all ControllerRevisions controlled by the given DaemonSet.
// This also reconciles ControllerRef by adopting/orphaning.
// Note that returned histories are pointers to objects in the cache.
// If you want to modify one, you need to deep-copy it first.
func (h *historyManager) controlledHistories(ctx context.Context, parent metav1.Object, labelSelector *metav1.LabelSelector) ([]*apps.ControllerRevision, error) {
	// List all histories to include those that don't match the selector anymore
	// but have a ControllerRef pointing to the controller.
	selector, err := metav1.LabelSelectorAsSelector(labelSelector)
	if err != nil {
		return nil, err
	}
	revisions, err := h.client.ListControllerRevisions(ctx, parent, selector)
	if err != nil {
		return nil, err
	}

	for i := range revisions {
		ownerRef := metav1.GetControllerOfNoCopy(revisions[i])
		if ownerRef != nil {
			continue
		}
		// adopt orhpan revision
		rv, err := h.client.AdoptControllerRevision(ctx, parent, h.revisionOwner.GetGroupVersionKind(), revisions[i])
		if err != nil {
			return revisions, err
		}
		revisions[i] = rv
	}

	return revisions, nil
}

func (rm *historyManager) cleanExpiredRevision(ctx context.Context, set metav1.Object, sortedRevisions *[]*apps.ControllerRevision) (*[]*apps.ControllerRevision, error) {
	limit := int(rm.revisionOwner.GetHistoryLimit(set))

	// reserve 2 extra unused revisions for diagnose
	exceedNum := len(*sortedRevisions) - limit - remainExtraUnusedRevisions
	if exceedNum <= 0 {
		return sortedRevisions, nil
	}

	inUsed, err := rm.revisionOwner.GetInUsedRevisions(set)
	if err != nil {
		return nil, err
	}
	var cleanedRevisions []*apps.ControllerRevision
	for _, revision := range *sortedRevisions {
		if exceedNum == 0 || inUsed.Has(revision.Name) {
			cleanedRevisions = append(cleanedRevisions, revision)
			continue
		}

		if err := rm.client.DeleteControllerRevision(ctx, revision); err != nil {
			return sortedRevisions, err
		}

		exceedNum--
	}

	return &cleanedRevisions, nil
}

// newRevision creates a new ControllerRevision containing a patch that reapplies the target state of set.
// The Revision of the returned ControllerRevision is set to revision. If the returned error is nil, the returned
// ControllerRevision is valid. StatefulSet revisions are stored as patches that re-apply the current state of set
// to a new StatefulSet using a strategic merge patch to replace the saved state of the new StatefulSet.
func (rm *historyManager) newRevision(set metav1.Object, revision int64, collisionCount *int32) (*apps.ControllerRevision, error) {
	patch, err := rm.revisionOwner.GetPatch(set)
	if err != nil {
		return nil, err
	}

	gvk := rm.revisionOwner.GetGroupVersionKind()
	revisionLabels := rm.revisionOwner.GetMatchLabels(set)

	cr, err := NewControllerRevision(set,
		gvk,
		revisionLabels,
		runtime.RawExtension{Raw: patch},
		revision,
		collisionCount)
	if err != nil {
		return nil, err
	}

	cr.Namespace = set.GetNamespace()

	return cr, nil
}
