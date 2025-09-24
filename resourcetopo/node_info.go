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
	"k8s.io/klog/v2"
)

var _ NodeInfo = &nodeInfo{}

type ownerInfo struct {
	metaKey string
	name    string
}

type nodeInfo struct {
	// immutable fields after node created
	storageRef *nodeStorage
	cluster    string
	namespace  string
	name       string

	// lock for PreOrders and PostOrders
	lock sync.RWMutex
	// Lists of nodeInfo ref, cached all existed relation for this node.
	directReferredPreOrders  *list.List
	labelReferredPreOrders   *list.List
	directReferredPostOrders *list.List
	labelReferredPostOrders  *list.List

	// only informer routine for this storage type will call metaLock.Lock()
	// other routines only call metaLock.RLock()
	metaLock   sync.RWMutex
	ownerNodes []ownerInfo
	labels     labels.Set // labels is a ref to object.meta.labels, do not edit!
	// objectExisted is added for directRef relation to cache the relation before post object added,
	// will be updated to true after the object added to cache or the manager is of virtual type.
	objectExisted bool

	// always call relationsLock.Lock() before relation change, so when locked, the relation is stable.
	// kind like a lock.RLock() in relation change scenario.
	relationsLock  sync.RWMutex
	labelRelations []ResourceRelation
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

func (node *nodeInfo) checkLabelUpdateForPostNode(postNode *nodeInfo) {
	if !node.isObjectExisted() {
		return
	}

	node.relationsLock.RLock()
	defer node.relationsLock.RUnlock()
	if len(node.labelRelations) == 0 {
		return
	}
	for _, relation := range node.labelRelations {
		if !typeEqual(relation.PostMeta, postNode.storageRef.meta) {
			return
		}
		selector, err := metav1.LabelSelectorAsSelector(relation.LabelSelector)
		if err != nil {
			klog.Errorf("Failed to resolve selector %v: %s", relation.LabelSelector, err.Error())
			return
		}
		if postNode.matched(selector) {
			if _, ok := node.storageRef.ownerRelation[postNode.storageRef.metaKey]; !ok || postNode.ownerMatched(node) {
				rangeAndSetLabelRelation(node, postNode, node.storageRef.manager)
				return
			}
		}
		if deleteLabelRelation(node, postNode) {
			postNode.noticePreOrderRelationDeleted(node)
		}
	}
}

func (n *nodeInfo) updateNodeMeta(obj Object) {
	n.metaLock.Lock()
	defer n.metaLock.Unlock()
	n.labels = obj.GetLabels()

	var ownerInfos []ownerInfo
	for _, ownerref := range obj.GetOwnerReferences() {
		ownerKey := generateKey(ownerref.APIVersion, ownerref.Kind)
		if !n.needRecordOwner(ownerKey) {
			continue
		}
		ownerInfos = append(ownerInfos, ownerInfo{metaKey: ownerKey, name: ownerref.Name})
	}
	n.ownerNodes = ownerInfos

	if !n.objectExisted {
		n.objectExisted = true
		n.lock.RLock()
		defer n.lock.RUnlock()
		rangeNodeList(n.directReferredPreOrders, func(preOrder *nodeInfo) {
			n.storageRef.manager.newRelationEvent(preOrder, n, EventTypeAdd)
		})
	}
}

func (n *nodeInfo) objectDeleted() {
	n.metaLock.Lock()
	defer n.metaLock.Unlock()

	n.objectExisted = false
	n.labels = nil
	n.ownerNodes = nil
}

func (n *nodeInfo) matched(selector labels.Selector) bool {
	n.metaLock.RLock()
	defer n.metaLock.RUnlock()
	return n.objectExisted && selector.Matches(n.labels)
}

func (n *nodeInfo) ownerMatched(node *nodeInfo) bool {
	n.metaLock.RLock()
	defer n.metaLock.RUnlock()
	for _, owner := range n.ownerNodes {
		if node.storageRef.metaKey == owner.metaKey &&
			node.name == owner.name {
			return true
		}
	}
	return false
}

func (n *nodeInfo) labelEqualed(labelMap map[string]string) bool {
	n.metaLock.RLock()
	defer n.metaLock.RUnlock()

	return maps.Equal(n.labels, labelMap)
}

func (n *nodeInfo) ownersEqualed(reference []metav1.OwnerReference) bool {
	n.metaLock.RLock()
	defer n.metaLock.RUnlock()

	if len(n.ownerNodes) > len(reference) {
		return false
	}
	idx := 0
	for _, owner := range reference {
		ownerKey := generateKey(owner.APIVersion, owner.Kind)
		if n.needRecordOwner(ownerKey) {
			if idx < len(n.ownerNodes) && owner.Name == n.ownerNodes[idx].name && ownerKey == n.ownerNodes[idx].metaKey {
				idx++
			} else {
				return false
			}
		}
	}
	return idx == len(n.ownerNodes)
}

func (n *nodeInfo) checkGC() {
	n.metaLock.RLock()
	defer n.metaLock.RUnlock()
	n.lock.RLock()
	defer n.lock.RUnlock()

	if !n.objectExisted &&
		// n maybe set as notExisted, but still have related nodes to handle
		(n.directReferredPreOrders == nil || n.directReferredPreOrders.Len() == 0) &&
		(n.directReferredPostOrders == nil || n.directReferredPostOrders.Len() == 0) &&
		(n.labelReferredPreOrders == nil || n.labelReferredPreOrders.Len() == 0) &&
		(n.labelReferredPostOrders == nil || n.labelReferredPostOrders.Len() == 0) {
		n.storageRef.deleteNode(n.cluster, n.namespace, n.name)
	}
}

func (n *nodeInfo) checkVirtualNodeGC() {
	if !n.storageRef.virtualResource {
		return
	}
	n.lock.RLock()
	defer n.lock.RUnlock()
	if n.directReferredPostOrders == nil || n.directReferredPostOrders.Len() == 0 {
		n.storageRef.deleteNode(n.cluster, n.namespace, n.name)
		n.storageRef.manager.newNodeEvent(n, EventTypeDelete)
	}
}

func (n *nodeInfo) isObjectExisted() bool {
	n.metaLock.RLock()
	defer n.metaLock.RUnlock()

	return n.objectExisted
}

func (n *nodeInfo) needRecordOwner(ownerKey string) bool {
	if ownerStorage := n.storageRef.preOrderResources[ownerKey]; ownerStorage == nil {
		return false
	} else {
		_, ok := ownerStorage.ownerRelation[n.storageRef.metaKey]
		return ok
	}
}

func (n *nodeInfo) noticePostOrderRelationDeleted(postOrder *nodeInfo) {
	if !postOrder.isObjectExisted() {
		return
	}
	m := n.storageRef.manager
	m.newRelationEvent(n, postOrder, EventTypeDelete)
	n.noticePostOrder(postOrder)
}

func (n *nodeInfo) noticePreOrderRelationDeleted(preOrder *nodeInfo) {
	m := n.storageRef.manager
	m.newRelationEvent(preOrder, n, EventTypeDelete)

	n.noticePreOrder(preOrder)
}

func (n *nodeInfo) noticePreOrder(preOrder *nodeInfo) {
	if _, ok := preOrder.storageRef.postNoticeRelation[n.storageRef.metaKey]; !ok {
		n.storageRef.manager.newNodeEvent(preOrder, EventTypeRelatedUpdate)
		preOrder.propagateNodeChange()
	}
}

func (n *nodeInfo) noticePostOrder(postOrder *nodeInfo) {
	if _, ok := n.storageRef.postNoticeRelation[postOrder.storageRef.metaKey]; ok {
		if !postOrder.isObjectExisted() {
			return
		}
		n.storageRef.manager.newNodeEvent(postOrder, EventTypeRelatedUpdate)
		postOrder.propagateNodeChange()
	}
}

// propagateNodeChange call node event handler for existed relations.
func (n *nodeInfo) propagateNodeChange() {
	// related nodes may be changed after RUnlock, but avoid RLock recursion caused deadlock
	preOrderNodes, postOrderNodes := list.New(), list.New()
	n.lock.RLock()
	rangeNodeList(n.directReferredPreOrders, func(preOrder *nodeInfo) { preOrderNodes.PushBack(preOrder) })
	rangeNodeList(n.labelReferredPreOrders, func(preOrder *nodeInfo) { preOrderNodes.PushBack(preOrder) })
	rangeNodeList(n.directReferredPostOrders, func(postOrder *nodeInfo) { postOrderNodes.PushBack(postOrder) })
	rangeNodeList(n.labelReferredPostOrders, func(postOrder *nodeInfo) { postOrderNodes.PushBack(postOrder) })
	n.lock.RUnlock()

	rangeNodeList(preOrderNodes, n.noticePreOrder)
	rangeNodeList(postOrderNodes, n.noticePostOrder)
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
