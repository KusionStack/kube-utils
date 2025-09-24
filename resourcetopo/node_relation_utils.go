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
	// In resourcetopo package, when do relation change operations,
	// we always lock preOrder before postOrder, expect in this function.
	// but to keep transition atomic, and in object deletion scenario,
	// this is okay to hold the lock in whole process, and lock preOrderNode as needed.
	// node.metaLock.Lock()
	// defer node.metaLock.Unlock()
	node.relationsLock.Lock()
	node.objectDeleted()
	node.labelRelations = nil
	defer node.relationsLock.Unlock()

	node.lock.Lock()
	defer node.lock.Unlock()

	rangeNodeList(node.directReferredPostOrders, func(postNode *nodeInfo) {
		postNode.lock.Lock()
		removeFromList(postNode.directReferredPreOrders, node)
		postNode.lock.Unlock()

		node.noticePostOrderRelationDeleted(postNode)
		postNode.checkGC()
	})
	node.directReferredPostOrders = nil

	rangeNodeList(node.labelReferredPostOrders, func(postNode *nodeInfo) {
		postNode.lock.Lock()
		removeFromList(postNode.labelReferredPreOrders, node)
		postNode.lock.Unlock()

		node.noticePostOrderRelationDeleted(postNode)
	})
	node.labelReferredPostOrders = nil

	// node has been set as deleted, labels set to nil,
	// so can not be locked as postOrder to add new label selector relation
	rangeNodeList(node.labelReferredPreOrders, func(preNode *nodeInfo) {
		preNode.lock.Lock()
		removeFromList(preNode.labelReferredPostOrders, node)
		preNode.lock.Unlock()
		node.noticePreOrderRelationDeleted(preNode)
	})
	node.labelReferredPreOrders = nil

	rangeNodeList(node.directReferredPreOrders, func(preNode *nodeInfo) {
		if preNode.storageRef.virtualResource {
			// virtual node will not start a relation pair change, so virtual preOrder node can be locked later
			preNode.lock.Lock()
			removeFromList(preNode.directReferredPostOrders, node)
			preNode.lock.Unlock()
			removeFromList(node.directReferredPreOrders, preNode)
			node.noticePreOrderRelationDeleted(preNode)
			preNode.checkVirtualNodeGC()
		} else {
			// notice relation deleted, but hold this place in case this object will be recreated
			node.noticePreOrderRelationDeleted(preNode)
		}
	})
}
