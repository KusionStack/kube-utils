/**
 * Copyright 2024 KusionStack Authors.
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

package resourcetopo

import (
	"container/list"
	"sync"

	"golang.org/x/exp/maps"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
)

var _ NodeInfo = &nodeInfo{}

type ownerInfo struct {
	metaKey string
	name    string
}

type nodeInfo struct {
	storageRef *nodeStorage
	lock       sync.RWMutex

	// Lists of nodeInfo ref, cached all existed relation for this node.
	directReferredPreOrders  *list.List
	labelReferredPreOrders   *list.List
	directReferredPostOrders *list.List
	labelReferredPostOrders  *list.List

	// We cached all the data needed by the relation existence decision.
	cluster    string
	namespace  string
	name       string
	ownerNodes []ownerInfo
	labels     labels.Set // labels is a ref to object.meta.labels, do not edit!
	relations  []ResourceRelation

	// objectExisted is added for directRef relation to cache the relation before post object added,
	// will be updated to true after the object added to cache or the manager is of virtual type.
	objectExisted bool
}

func newNode(s *nodeStorage, cluster, namespace, name string) *nodeInfo {
	return &nodeInfo{
		storageRef: s,
		cluster:    cluster,
		namespace:  namespace,
		name:       name,
		// For virtual resource, its lifecycle is handled by resourcetopo
		objectExisted: s.virtualResource,
	}
}

// TypeInfo return the node's resource type info
func (n *nodeInfo) TypeInfo() metav1.TypeMeta {
	return n.storageRef.meta
}

// NodeInfo return the node's namespaced name
func (n *nodeInfo) NodeInfo() types.NamespacedName {
	return types.NamespacedName{
		Namespace: n.namespace,
		Name:      n.name,
	}
}

func (n *nodeInfo) Cluster() string {
	return n.cluster
}

// GetPreOrders return the pre-order node slice for this node
func (n *nodeInfo) GetPreOrders() []NodeInfo {
	n.lock.RLock()
	defer n.lock.RUnlock()
	return append(transformToNodeSliceWithFilters(n.labelReferredPreOrders, objectExistFilter),
		transformToNodeSliceWithFilters(n.directReferredPreOrders, objectExistFilter)...)
}

// GetPostOrders return the post-order node slice for this node
func (n *nodeInfo) GetPostOrders() []NodeInfo {
	n.lock.RLock()
	defer n.lock.RUnlock()
	return append(transformToNodeSliceWithFilters(n.labelReferredPostOrders, objectExistFilter),
		transformToNodeSliceWithFilters(n.directReferredPostOrders, objectExistFilter)...)
}

// GetPreOrdersWithMeta return the pre-order nodes slice that match this resource meta
func (n *nodeInfo) GetPreOrdersWithMeta(meta metav1.TypeMeta) []NodeInfo {
	n.lock.RLock()
	defer n.lock.RUnlock()
	return append(transformToNodeSliceWithFilters(n.labelReferredPreOrders, metaMatchFillter(meta), objectExistFilter),
		transformToNodeSliceWithFilters(n.directReferredPreOrders, metaMatchFillter(meta), objectExistFilter)...)
}

// GetPostOrdersWithMeta return the post-order node slice that match this resource meta
func (n *nodeInfo) GetPostOrdersWithMeta(meta metav1.TypeMeta) []NodeInfo {
	n.lock.RLock()
	defer n.lock.RUnlock()
	return append(transformToNodeSliceWithFilters(n.labelReferredPostOrders, metaMatchFillter(meta), objectExistFilter),
		transformToNodeSliceWithFilters(n.directReferredPostOrders, metaMatchFillter(meta), objectExistFilter)...)
}

func (n *nodeInfo) updateNodeMeta(obj Object) {
	n.lock.Lock()
	defer n.lock.Unlock()
	n.labels = obj.GetLabels()
	n.resolveOwner(obj)
	if !n.objectExisted {
		n.postObjectAdded()
		n.objectExisted = true
	}
}

func (n *nodeInfo) matched(selector labels.Selector) bool {
	n.lock.RLock()
	defer n.lock.RUnlock()
	return selector.Matches(n.labels)
}

func (n *nodeInfo) ownerMatched(node *nodeInfo) bool {
	n.lock.RLock()
	defer n.lock.RUnlock()
	for _, owner := range n.ownerNodes {
		if node.storageRef.metaKey == owner.metaKey &&
			node.name == owner.name {
			return true
		}
	}
	return false
}

func (n *nodeInfo) labelEqualed(labelMap map[string]string) bool {
	n.lock.RLock()
	defer n.lock.RUnlock()

	return maps.Equal(n.labels, labelMap)
}

// preOrderRelationDeleted do sth when preNode changed and trigger this relation delete event
func (n *nodeInfo) preOrderRelationDeleted(preNode *nodeInfo) {
	// for direct referred relation,
	if !n.isObjectExisted() {
		return
	}
	m := n.storageRef.manager
	m.newRelationEvent(preNode, n, EventTypeDelete)

	if _, ok := preNode.storageRef.postNoticeRelation[n.storageRef.metaKey]; ok {
		m.newNodeEvent(n, EventTypeRelatedUpdate)
		n.propagateNodeChange(m)
	}
}

// postOrderRelationDeleted do sth when postNode changed and trigger this relation delete event
func (n *nodeInfo) postOrderRelationDeleted(postNode *nodeInfo) {
	m := n.storageRef.manager
	m.newRelationEvent(n, postNode, EventTypeDelete)

	if _, ok := n.storageRef.postNoticeRelation[postNode.storageRef.metaKey]; !ok {
		m.newNodeEvent(n, EventTypeRelatedUpdate)
		n.propagateNodeChange(m)
	}
}

// propagateNodeChange call node event handler for existed relations.
func (n *nodeInfo) propagateNodeChange(m *manager) {
	n.lock.RLock()
	defer n.lock.RUnlock()

	noticePreOrder := func(preOrder *nodeInfo) {
		if _, ok := preOrder.storageRef.postNoticeRelation[n.storageRef.metaKey]; !ok {
			m.newNodeEvent(preOrder, EventTypeRelatedUpdate)
			preOrder.propagateNodeChange(m)
		}
	}
	noticePostOrder := func(postOrder *nodeInfo) {
		if _, ok := n.storageRef.postNoticeRelation[postOrder.storageRef.metaKey]; ok {
			if !postOrder.isObjectExisted() {
				return
			}
			m.newNodeEvent(postOrder, EventTypeRelatedUpdate)
			postOrder.propagateNodeChange(m)
		}
	}

	rangeNodeList(n.directReferredPreOrders, noticePreOrder)
	rangeNodeList(n.labelReferredPreOrders, noticePreOrder)
	rangeNodeList(n.directReferredPostOrders, noticePostOrder)
	rangeNodeList(n.labelReferredPostOrders, noticePostOrder)

	if n.storageRef.virtualResource {
		if n.directReferredPostOrders == nil || n.directReferredPostOrders.Len() == 0 {
			n.storageRef.deleteNode(n.cluster, n.namespace, n.name)
			m.newNodeEvent(n, EventTypeDelete)
		}
	} else if n.readyToDelete() {
		n.storageRef.deleteNode(n.cluster, n.namespace, n.name)
	}
}

func (n *nodeInfo) readyToDelete() bool {
	n.lock.RLock()
	defer n.lock.RUnlock()
	return !n.objectExisted && (n.directReferredPreOrders == nil || n.directReferredPreOrders.Len() == 0)
}

func (n *nodeInfo) isObjectExisted() bool {
	n.lock.RLock()
	defer n.lock.RUnlock()

	return n.objectExisted
}

func (n *nodeInfo) postObjectAdded() {
	if n.directReferredPreOrders != nil {
		for ele := n.directReferredPreOrders.Front(); ele != nil; ele = ele.Next() {
			preOrder := ele.Value.(*nodeInfo)
			n.storageRef.manager.newRelationEvent(preOrder, n, EventTypeAdd)
		}
	}
}

func (n *nodeInfo) preObjectDeleted() {
	n.objectExisted = false
	n.labels = nil
}

func (n *nodeInfo) resolveOwner(o Object) {
	for _, ownerref := range o.GetOwnerReferences() {
		ownerKey := generateKey(ownerref.APIVersion, ownerref.Kind)
		ownerStorage := n.storageRef.preOrderResources[ownerKey]
		if ownerStorage == nil {
			continue
		}
		if _, ok := ownerStorage.ownerRelation[n.storageRef.metaKey]; !ok {
			continue
		}

		n.ownerNodes = append(n.ownerNodes, ownerInfo{
			metaKey: ownerKey,
			name:    ownerref.Name,
		})
	}
}

func metaMatchFillter(meta metav1.TypeMeta) func(info *nodeInfo) bool {
	return func(info *nodeInfo) bool {
		return info.storageRef.meta.APIVersion == meta.APIVersion &&
			info.storageRef.meta.Kind == meta.Kind
	}
}

func objectExistFilter(info *nodeInfo) bool {
	return info.isObjectExisted()
}
