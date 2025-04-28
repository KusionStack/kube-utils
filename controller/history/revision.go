/*
Copyright 2016 The Kubernetes Authors.
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
	"fmt"
	"hash"
	"hash/fnv"
	"sort"
	"strconv"
	"sync"

	apps "k8s.io/api/apps/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/rand"
	"kusionstack.io/kube-utils/controller/names"
)

const (
	// ControllerRevisionHashLabel is the label used to indicate
	// the hash of a specific revision.
	ControllerRevisionHashLabel = "controller.kubernetes.io/hash"

	// ControllerRevisionCollisionLabel is the label used to indicate
	// collision count number used for generating hash.
	ControllerRevisionCollisionLabel = "controller.kubernetes.io/collision"
)

// ControllerRevisionName returns the Name for a ControllerRevision in the form prefix-hash.
// The ControllerRevisionName sometimes will be used as a label value so it must be no more than 63 characters.
//
// If the length of final name is greater than 63 bytes, the prefix is truncated to allow for a name
// that is no larger than 63 bytes.
func ControllerRevisionName(prefix, hash string) string {
	return names.GenerateDNS1035Label(prefix, hash)
}

// HashControllerRevision hashes the contents of revision's Data using FNV hashing. If probe is not nil, the byte value
// of probe is added written to the hash as well. The returned hash will be a safe encoded string to avoid bad words.
func HashControllerRevision(revision *apps.ControllerRevision, probe *int32) string {
	return HashControllerRevisionRawData(revision.Data.Raw, probe)
}

var hashPool = sync.Pool{
	New: func() interface{} {
		return fnv.New32()
	},
}

// HashControllerRevisionRawData hashes the raw data bytes using FNV hashing. If probe is not nil, the byte value
// of probe is added written to the hash as well. The returned hash will be a safe encoded string to avoid bad words.
func HashControllerRevisionRawData(data []byte, probe *int32) string {
	hf := hashPool.Get().(hash.Hash32)
	defer func() {
		hashPool.Put(hf)
	}()
	hf.Reset()
	if len(data) > 0 {
		hf.Write(data)
	}
	if probe != nil {
		hf.Write([]byte(strconv.FormatInt(int64(*probe), 10)))
	}
	return rand.SafeEncodeString(fmt.Sprint(hf.Sum32()))
}

// SortControllerRevisions sorts revisions by their Revision.
func SortControllerRevisions(revisions []*apps.ControllerRevision) {
	sort.Sort(byRevision(revisions))
}

// byRevision implements sort.Interface to allow ControllerRevisions to be sorted by Revision.
type byRevision []*apps.ControllerRevision

func (br byRevision) Len() int {
	return len(br)
}

func (br byRevision) Less(i, j int) bool {
	return br[i].Revision < br[j].Revision
}

func (br byRevision) Swap(i, j int) {
	br[i], br[j] = br[j], br[i]
}

// EqualRevision returns true if lhs and rhs are either both nil, or both have same labels and annotations, or bath point
// to non-nil ControllerRevisions that contain semantically equivalent data. Otherwise this method returns false.
func EqualRevision(lhs, rhs *apps.ControllerRevision) bool {
	var lhsHash, rhsHash *uint32
	if lhs == nil || rhs == nil {
		return lhs == rhs
	}

	if hs, found := lhs.Labels[ControllerRevisionHashLabel]; found {
		hash, err := strconv.ParseInt(hs, 10, 32)
		if err == nil {
			lhsHash = new(uint32)
			*lhsHash = uint32(hash)
		}
	}
	if hs, found := rhs.Labels[ControllerRevisionHashLabel]; found {
		hash, err := strconv.ParseInt(hs, 10, 32)
		if err == nil {
			rhsHash = new(uint32)
			*rhsHash = uint32(hash)
		}
	}

	if lhsHash != nil && rhsHash != nil && *lhsHash != *rhsHash {
		return false
	}
	return bytes.Equal(lhs.Data.Raw, rhs.Data.Raw) && apiequality.Semantic.DeepEqual(lhs.Data.Object, rhs.Data.Object)
}

// FindEqualRevisions returns all ControllerRevisions in revisions that are equal to needle using EqualRevision as the
// equality test. The returned slice preserves the order of revisions.
func FindEqualRevisions(revisions []*apps.ControllerRevision, needle *apps.ControllerRevision) []*apps.ControllerRevision {
	var eq []*apps.ControllerRevision
	for i := range revisions {
		if EqualRevision(revisions[i], needle) {
			eq = append(eq, revisions[i])
		}
	}
	return eq
}

// nextRevision finds the next valid revision number based on revisions. If the length of revisions
// is 0 this is 1. Otherwise, it is 1 greater than the largest revision's Revision. This method
// assumes that revisions has been sorted by Revision.
func nextRevision(revisions []*apps.ControllerRevision) int64 {
	count := len(revisions)
	if count <= 0 {
		return 1
	}
	return revisions[count-1].Revision + 1
}

// NewControllerRevision returns a ControllerRevision with a ControllerRef pointing to parent and indicating that
// parent is of parentKind. The ControllerRevision has labels matching template labels, contains Data equal to data, and
// has a Revision equal to revision. The collisionCount is used when creating the name of the ControllerRevision
// so the name is likely unique. If the returned error is nil, the returned ControllerRevision is valid. If the
// returned error is not nil, the returned ControllerRevision is invalid for use.
func NewControllerRevision(parent metav1.Object,
	parentKind schema.GroupVersionKind,
	templateLabels map[string]string,
	data runtime.RawExtension,
	revision int64,
	collisionCount *int32,
) (*apps.ControllerRevision, error) {
	labelMap := make(map[string]string)
	for k, v := range templateLabels {
		labelMap[k] = v
	}
	blockOwnerDeletion := true
	isController := true
	cr := &apps.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{
			Labels: labelMap,
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion:         parentKind.GroupVersion().String(),
					Kind:               parentKind.Kind,
					Name:               parent.GetName(),
					UID:                parent.GetUID(),
					BlockOwnerDeletion: &blockOwnerDeletion,
					Controller:         &isController,
				},
			},
		},
		Data:     data,
		Revision: revision,
	}
	hash := HashControllerRevision(cr, collisionCount)
	cr.Name = ControllerRevisionName(parent.GetName(), hash)
	cr.Labels[ControllerRevisionHashLabel] = hash
	cr.Labels[ControllerRevisionCollisionLabel] = strconv.Itoa(int(*collisionCount))

	if ns := parent.GetNamespace(); len(ns) > 0 {
		cr.Namespace = ns
	}
	return cr, nil
}
