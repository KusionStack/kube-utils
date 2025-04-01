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
	"golang.org/x/exp/slices"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
)

var _ cache.ResourceEventHandler = &nodeStorage{}

func (s *nodeStorage) OnAdd(obj interface{}) {
	topoObject, ok := obj.(Object)
	if !ok {
		klog.Errorf("Failed to transform to k8s object %v, ignore this add nodeEvent", obj)
		return
	}
	node := s.getOrCreateNode(getObjectCluster(topoObject), topoObject.GetNamespace(), topoObject.GetName())
	s.addNode(topoObject, node)

	s.manager.newNodeEvent(node, EventTypeAdd)
	node.propagateNodeChange(s.manager)
}

func (s *nodeStorage) OnUpdate(oldObj, newObj interface{}) {
	newTopoObj, ok := newObj.(Object)
	if !ok {
		klog.Errorf("Failed to transform to k8s object %v, ignore this update nodeEvent", newObj)
		return
	}
	oldTopoObj, ok := oldObj.(Object)
	if !ok {
		klog.Errorf("Failed to transform to k8s object %v, ignore this update nodeEvent", oldObj)
		return
	}
	cluster := getObjectCluster(newTopoObj)

	node := s.getNode(cluster, newTopoObj.GetNamespace(), newTopoObj.GetName())
	if node == nil {
		node = s.createNode(cluster, newTopoObj.GetNamespace(), newTopoObj.GetName())
		s.addNode(newTopoObj, node)
		return
	}

	var resolvedRelations []ResourceRelation
	for _, resolver := range s.resolvers {
		relations := resolver.Resolve(newTopoObj)
		resolvedRelations = append(resolvedRelations, relations...)
	}

	slices.SortFunc(resolvedRelations, compareResourceRelation)
	node.relationsLock.Lock()
	sortedSlicesCompare(node.relations, resolvedRelations,
		func(relation ResourceRelation) {
			s.removeResourceRelation(node, &relation)
		},
		func(relation ResourceRelation) {
			s.addResourceRelation(node, &relation)
		},
		compareResourceRelation)
	node.relations = resolvedRelations
	node.relationsLock.Unlock()

	if !node.labelEqualed(newTopoObj.GetLabels()) {
		node.updateNodeMeta(newTopoObj)
		for _, preStorage := range s.preOrderResources {
			preStorage.checkForLabelUpdate(node)
		}
	}

	for _, discover := range s.discoverers {
		newDiscoverd := discover.Discover(newTopoObj)
		oldDiscoverd := discover.Discover(oldTopoObj)
		slices.SortFunc(newDiscoverd, compareNodeName)
		slices.SortFunc(oldDiscoverd, compareNodeName)

		discoveredStorage := s.manager.getStorage(discover.PreMeta)
		sortedSlicesCompare(newDiscoverd, oldDiscoverd,
			func(name types.NamespacedName) {
				discoveredNode := discoveredStorage.getOrCreateNode(cluster, name.Namespace, name.Name)
				addDirectRefRelation(discoveredNode, node, s.manager)
			},
			func(namespacedName types.NamespacedName) {
				discoveredNode := discoveredStorage.getNode(cluster, namespacedName.Namespace, namespacedName.Name)
				if deleteDirectRelation(discoveredNode, node) {
					// deleted relation will not be called by later node.propagateNodeChange
					discoveredNode.postOrderRelationDeleted(node)
				}
			},
			compareNodeName)
	}

	s.manager.newNodeEvent(node, EventTypeUpdate)
	node.propagateNodeChange(s.manager)
}

func (s *nodeStorage) OnDelete(obj interface{}) {
	topoObject, ok := obj.(metav1.Object)
	if !ok {
		klog.Errorf("Failed to transform to k8s object %v, ignore this delete nodeEvent", obj)
		return
	}
	cluster := getObjectCluster(topoObject)

	node := s.getNode(cluster, topoObject.GetNamespace(), topoObject.GetName())
	if node == nil {
		return
	}

	node.preObjectDeleted()
	deleteAllRelation(node)
	if node.readyToDelete() {
		s.deleteNode(cluster, node.namespace, node.name)
	}

	s.manager.newNodeEvent(node, EventTypeDelete)
}

func (s *nodeStorage) addNode(obj Object, node *nodeInfo) {
	node.updateNodeMeta(obj)
	if len(node.relations) != 0 {
		klog.Warningf("unexpected relations {%v}", node.relations)
		node.relations = nil
	}
	node.relationsLock.Lock()
	for _, resolver := range s.resolvers {
		relations := resolver.Resolve(obj)
		node.relations = append(node.relations, relations...)
	}
	slices.SortFunc(node.relations, compareResourceRelation)
	for _, relation := range node.relations {
		s.addResourceRelation(node, &relation)
	}
	node.relationsLock.Unlock()

	for _, discoverer := range s.discoverers {
		preStorage := s.manager.getStorage(discoverer.PreMeta)
		preObjs := discoverer.Discover(obj)
		for _, preObj := range preObjs {
			preNode := preStorage.getOrCreateNode(node.cluster, preObj.Namespace, preObj.Name)
			addDirectRefRelation(preNode, node, s.manager)
		}
	}

	for _, preOrderStorage := range s.preOrderResources {
		preOrderStorage.checkForLabelUpdate(node)
	}
}

func (s *nodeStorage) addResourceRelation(node *nodeInfo, relation *ResourceRelation) {
	postMeta := relation.PostMeta
	postMetaKey := generateMetaKey(postMeta)
	postStorage := s.manager.getStorage(postMeta)
	if postStorage == nil {
		klog.Errorf("Failed to get node storage by meta %s, ignore this relation", postMetaKey)
		return
	}
	if len(relation.DirectRefs) > 0 {
		for _, ref := range relation.DirectRefs {
			postNode := postStorage.getOrCreateNode(relation.Cluster, ref.Namespace, ref.Name)
			rangeAndSetDirectRefRelation(node, postNode, s.manager)
		}
	}

	if relation.LabelSelector != nil {
		var postNodes []*nodeInfo
		if _, ok := s.ownerRelation[postMetaKey]; ok {
			postNodes = postStorage.getMatchedNodeListWithOwner(node.cluster, node.namespace, relation.LabelSelector, node)
		} else {
			postNodes = postStorage.getMatchedNodeList(node.cluster, node.namespace, relation.LabelSelector)
		}
		for _, postNode := range postNodes {
			rangeAndSetLabelRelation(node, postNode, s.manager)
		}
	}
}

func (s *nodeStorage) removeResourceRelation(node *nodeInfo, relation *ResourceRelation) {
	postStorage := s.manager.getStorage(relation.PostMeta)
	if postStorage == nil {
		klog.Error("Failed to get node Storage by %s, ignore this delete request",
			generateMetaKey(relation.PostMeta))
		return
	}
	if len(relation.DirectRefs) > 0 {
		for _, ref := range relation.DirectRefs {
			postNode := postStorage.getNode(relation.Cluster, ref.Namespace, ref.Name)
			if deleteDirectRelation(node, postNode) {
				postNode.preOrderRelationDeleted(node)
			}
		}
	}

	if relation.LabelSelector != nil {
		postNodes := postStorage.getMatchedNodeList(node.cluster, node.namespace, relation.LabelSelector)
		for _, postNode := range postNodes {
			if deleteLabelRelation(node, postNode) {
				postNode.preOrderRelationDeleted(node)
			}
		}
	}
}
