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
)

func rangeAndSetLabelRelation(preNode, postNode *nodeInfo, m *manager) {
	// relation add should be an atomic action
	preNode.lock.Lock()
	defer preNode.lock.Unlock()
	if existInList(preNode.labelReferredPostOrders, postNode) {
		return
	}
	postNode.lock.Lock()
	defer postNode.lock.Unlock()

	if postNode.labelReferredPreOrders == nil {
		postNode.labelReferredPreOrders = list.New()
	}
	if preNode.labelReferredPostOrders == nil {
		preNode.labelReferredPostOrders = list.New()
	}
	postNode.labelReferredPreOrders.PushBack(preNode)
	preNode.labelReferredPostOrders.PushBack(postNode)
	m.newRelationEvent(preNode, postNode, EventTypeAdd)
}

func rangeAndSetDirectRefRelation(preNode, postNode *nodeInfo, m *manager) {
	preNode.lock.Lock()
	defer preNode.lock.Unlock()
	if existInList(preNode.directReferredPostOrders, postNode) {
		return
	}
	postNode.lock.Lock()
	defer postNode.lock.Unlock()

	if postNode.directReferredPreOrders == nil {
		postNode.directReferredPreOrders = list.New()
	}
	if preNode.directReferredPostOrders == nil {
		preNode.directReferredPostOrders = list.New()
	}

	// relation add should be an atomic action
	postNode.directReferredPreOrders.PushBack(preNode)
	preNode.directReferredPostOrders.PushBack(postNode)

	if postNode.isObjectExisted() {
		m.newRelationEvent(preNode, postNode, EventTypeAdd)
	}
}

// deleteLabelRelation return true if this relation existed and removed successful.
// for relation delete condition, postDelete action need to be called by caller.
func deleteLabelRelation(preNode, postNode *nodeInfo) bool {
	// relation delete should be an atomic action
	preNode.lock.Lock()
	defer preNode.lock.Unlock()
	postNode.lock.Lock()
	defer postNode.lock.Unlock()

	return removeFromList(preNode.labelReferredPostOrders, postNode) &&
		(removeFromList(postNode.labelReferredPreOrders, preNode))
}

// deleteDirectRelation return true if this relation existed and removed successful.
// for relation delete condition, postDelete action need to be called by caller.
func deleteDirectRelation(preNode, postNode *nodeInfo) bool {
	// relation delete should be an atomic action
	preNode.lock.Lock()
	defer preNode.lock.Unlock()
	postNode.lock.Lock()
	defer postNode.lock.Unlock()

	return removeFromList(preNode.directReferredPostOrders, postNode) &&
		removeFromList(postNode.directReferredPreOrders, preNode)
}

// deleteAllRelation will remove all relation in this node.
// only be called in object deletion scenario.
func deleteAllRelation(node *nodeInfo) {
	// First, mark object as deleted under metaLock (Level 6 - innermost)
	node.objectDeleted()

	// Collect all related nodes before acquiring locks to avoid holding locks during iteration
	var directPostNodes, labelPostNodes, labelPreNodes, directPreNodes []*nodeInfo
	node.lock.RLock()
	rangeNodeList(node.directReferredPostOrders, func(postNode *nodeInfo) {
		directPostNodes = append(directPostNodes, postNode)
	})
	rangeNodeList(node.labelReferredPostOrders, func(postNode *nodeInfo) {
		labelPostNodes = append(labelPostNodes, postNode)
	})
	rangeNodeList(node.labelReferredPreOrders, func(preNode *nodeInfo) {
		labelPreNodes = append(labelPreNodes, preNode)
	})
	rangeNodeList(node.directReferredPreOrders, func(preNode *nodeInfo) {
		directPreNodes = append(directPreNodes, preNode)
	})
	node.lock.RUnlock()

	// Process post-nodes (node is preOrder, so lock node first, then postNode)
	// This follows the rule: preNode.lock before postNode.lock
	for _, postNode := range directPostNodes {
		deleteDirectRelation(node, postNode)
		node.noticePostOrderRelationDeleted(postNode)
		postNode.checkGC()
	}

	for _, postNode := range labelPostNodes {
		deleteLabelRelation(node, postNode)
		node.noticePostOrderRelationDeleted(postNode)
	}

	// Process pre-nodes (preNode is preOrder, so lock preNode first, then node)
	// This follows the rule: preNode.lock before postNode.lock
	for _, preNode := range labelPreNodes {
		// For label relations, node is postOrder, so we need preNode.lock first
		preNode.lock.Lock()
		removeFromList(preNode.labelReferredPostOrders, node)
		preNode.lock.Unlock()
		node.noticePreOrderRelationDeleted(preNode)
	}

	for _, preNode := range directPreNodes {
		if preNode.storageRef.virtualResource {
			// For virtual resources, we can modify the relation directly
			preNode.lock.Lock()
			removeFromList(preNode.directReferredPostOrders, node)
			preNode.lock.Unlock()
			// Also update node's list while holding node's lock
			node.lock.Lock()
			removeFromList(node.directReferredPreOrders, preNode)
			node.lock.Unlock()
			node.noticePreOrderRelationDeleted(preNode)
			preNode.checkVirtualNodeGC()
		} else {
			// For non-virtual resources, just notify but keep node's list as-is
			// in case this object will be recreated
			node.noticePreOrderRelationDeleted(preNode)
		}
	}

	// Clean up node's relation lists and labelRelations under proper locks
	node.relationsLock.Lock()
	node.labelRelations = nil
	node.relationsLock.Unlock()

	node.lock.Lock()
	node.directReferredPostOrders = nil
	node.labelReferredPostOrders = nil
	node.labelReferredPreOrders = nil
	// Keep directReferredPreOrders for non-virtual preNodes in case of recreation
	node.lock.Unlock()
}
