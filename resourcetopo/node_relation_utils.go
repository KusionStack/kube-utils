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

import "container/list"

func rangeAndSetLabelRelation(preNode, postNode *nodeInfo, m *manager) {
	if labelRelationExist(preNode, postNode) {
		return
	}
	addLabelRelation(preNode, postNode, m)
}

func rangeAndSetDirectRefRelation(preNode, postNode *nodeInfo, m *manager) {
	if directRelationExist(preNode, postNode) {
		return
	}
	addDirectRefRelation(preNode, postNode, m)
}

func addLabelRelation(preNode, postNode *nodeInfo, m *manager) {
	if postNode.labelReferredPreOrders == nil {
		postNode.labelReferredPreOrders = list.New()
	}
	if preNode.labelReferredPostOrders == nil {
		preNode.labelReferredPostOrders = list.New()
	}

	// relation add should be an atomic action
	preNode.lock.Lock()
	defer preNode.lock.Unlock()
	postNode.lock.Lock()
	defer postNode.lock.Unlock()

	postNode.labelReferredPreOrders.PushBack(preNode)
	preNode.labelReferredPostOrders.PushBack(postNode)

	m.newRelationEvent(preNode, postNode, EventTypeAdd)
}

func addDirectRefRelation(preNode, postNode *nodeInfo, m *manager) {
	if postNode.directReferredPreOrders == nil {
		postNode.directReferredPreOrders = list.New()
	}
	if preNode.directReferredPostOrders == nil {
		preNode.directReferredPostOrders = list.New()
	}

	// relation add should be an atomic action
	preNode.lock.Lock()
	postNode.lock.Lock()
	postNode.directReferredPreOrders.PushBack(preNode)
	preNode.directReferredPostOrders.PushBack(postNode)
	postNode.lock.Unlock()
	preNode.lock.Unlock()

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

func deleteAllRelation(node *nodeInfo) {
	node.lock.Lock()

	rangeNodeList(node.directReferredPostOrders, func(postNode *nodeInfo) {
		postNode.lock.Lock()
		removeFromList(postNode.directReferredPreOrders, node)
		postNode.lock.Unlock()

		postNode.preOrderRelationDeleted(node)
	})
	node.directReferredPostOrders = nil

	rangeNodeList(node.labelReferredPostOrders, func(postNode *nodeInfo) {
		postNode.lock.Lock()
		removeFromList(postNode.labelReferredPreOrders, node)
		postNode.lock.Unlock()

		postNode.preOrderRelationDeleted(node)
	})
	node.labelReferredPostOrders = nil

	node.lock.Unlock()

	// In case of deadlock, we will always expect to lock pre node before post node.
	for {
		node.lock.RLock()
		if node.labelReferredPreOrders == nil || node.labelReferredPreOrders.Len() == 0 {
			node.labelReferredPreOrders = nil
			node.lock.RUnlock()
			break
		} else {
			preNode := node.labelReferredPreOrders.Front().Value.(*nodeInfo)
			node.lock.RUnlock()

			deleteLabelRelation(preNode, node)
			preNode.postOrderRelationDeleted(node)
		}
	}

	var virtualNodelist = list.New()
	node.lock.RLock()
	rangeNodeList(node.directReferredPreOrders, func(preNode *nodeInfo) {
		if preNode.storageRef.virtualResource {
			// remember virtual nodes to delete later.
			virtualNodelist.PushBack(preNode)
		} else {
			// notice relation deleted, but hold this place in case this object will be recreated
			preNode.postOrderRelationDeleted(node)
		}
	})
	node.lock.RUnlock()
	rangeNodeList(virtualNodelist, func(preNode *nodeInfo) {
		deleteDirectRelation(preNode, node)
		preNode.postOrderRelationDeleted(node)
	})
}

func labelRelationExist(preOrder, postOrder *nodeInfo) bool {
	preOrder.lock.RLock()
	defer preOrder.lock.RUnlock()
	postOrder.lock.RLock()
	defer postOrder.lock.RUnlock()

	return existInList(preOrder.labelReferredPostOrders, postOrder)
}

func directRelationExist(preOrder, postOrder *nodeInfo) bool {
	preOrder.lock.RLock()
	defer preOrder.lock.RUnlock()
	postOrder.lock.RLock()
	defer postOrder.lock.RUnlock()

	return existInList(preOrder.directReferredPostOrders, postOrder)
}
