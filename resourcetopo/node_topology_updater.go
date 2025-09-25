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

type nodeMeta struct {
	apiVersion, kind         string
	cluster, namespace, name string
}

var _ cache.ResourceEventHandler = &nodeStorage{}

func (s *nodeStorage) OnAdd(obj interface{}) {
	topoObject, ok := obj.(Object)
	if !ok {
		klog.Errorf("Failed to transform to k8s object %v, ignore this add nodeEvent", obj)
		return
	}
	klog.V(6).Infof("OnAdd started %s %s/%s", s.metaKey, topoObject.GetNamespace(), topoObject.GetName())

	node := s.getOrCreateNode(getObjectCluster(topoObject), topoObject.GetNamespace(), topoObject.GetName())
	s.addNode(topoObject, node)

	s.manager.newNodeEvent(node, EventTypeAdd)
	node.propagateNodeChange()
	klog.V(6).Infof("OnAdd finished %s %s/%s", s.metaKey, topoObject.GetNamespace(), topoObject.GetName())
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
	klog.V(6).Infof("OnUpdate started %s %s/%s", s.metaKey, newTopoObj.GetNamespace(), newTopoObj.GetName())
	cluster := getObjectCluster(newTopoObj)

	node := s.getNode(cluster, newTopoObj.GetNamespace(), newTopoObj.GetName())
	if node == nil {
		node = s.createNode(cluster, newTopoObj.GetNamespace(), newTopoObj.GetName())
		s.addNode(newTopoObj, node)
		return
	}

	var resolvedLabelRelations []ResourceRelation
	resolvedDirectRelations := make(map[nodeMeta]interface{})
	for _, resolver := range s.resolvers {
		relations := resolver.Resolve(newTopoObj)
		for _, relation := range relations {
			if relation.LabelSelector != nil {
				resolvedLabelRelations = append(resolvedLabelRelations, relation)
			} else {
				for _, directRef := range relation.DirectRefs {
					resolvedDirectRelations[nodeMeta{
						apiVersion: relation.PostMeta.APIVersion,
						kind:       relation.PostMeta.Kind,
						cluster:    relation.Cluster,
						namespace:  directRef.Namespace,
						name:       directRef.Name,
					}] = nil
				}
			}
		}
	}

	slices.SortFunc(resolvedLabelRelations, compareLabelResourceRelation)
	node.relationsLock.Lock()
	sortedSlicesCompare(node.labelRelations, resolvedLabelRelations,
		func(relation ResourceRelation) {
			s.removeLabelResourceRelation(node, &relation)
		},
		func(relation ResourceRelation) {
			s.addLabelResourceRelation(node, &relation)
		},
		compareLabelResourceRelation)
	node.labelRelations = resolvedLabelRelations

	s.setDirectResourceRelation(node, resolvedDirectRelations)
	node.relationsLock.Unlock()

	if !node.labelEqualed(newTopoObj.GetLabels()) ||
		!node.ownersEqualed(newTopoObj.GetOwnerReferences()) {
		node.updateNodeMeta(newTopoObj)
		for _, preStorage := range s.preOrderResources {
			preStorage.checkForLabelUpdate(node)
		}
	}

	for _, discover := range s.discoverers {
		newDiscovered := discover.Discover(newTopoObj)
		oldDiscovered := discover.Discover(oldTopoObj)
		slices.SortFunc(newDiscovered, compareNodeName)
		slices.SortFunc(oldDiscovered, compareNodeName)

		discoveredStorage := s.manager.getStorage(discover.PreMeta)
		sortedSlicesCompare(newDiscovered, oldDiscovered,
			func(name types.NamespacedName) {
				discoveredNode := discoveredStorage.getOrCreateNode(cluster, name.Namespace, name.Name)
				rangeAndSetDirectRefRelation(discoveredNode, node, s.manager)
			},
			func(namespacedName types.NamespacedName) {
				discoveredNode := discoveredStorage.getNode(cluster, namespacedName.Namespace, namespacedName.Name)
				if deleteDirectRelation(discoveredNode, node) {
					// deleted relation will not be called by later node.propagateNodeChange
					node.noticePreOrderRelationDeleted(discoveredNode)
					discoveredNode.checkVirtualNodeGC()
				}
			},
			compareNodeName)
	}

	s.manager.newNodeEvent(node, EventTypeUpdate)
	node.propagateNodeChange()
	klog.V(6).Infof("OnUpdate finished  %s %s/%s", s.metaKey, newTopoObj.GetNamespace(), newTopoObj.GetName())
}

func (s *nodeStorage) OnDelete(obj interface{}) {
	topoObject, ok := obj.(metav1.Object)
	if !ok {
		klog.Errorf("Failed to transform to k8s object %v, ignore this delete nodeEvent", obj)
		return
	}
	klog.V(6).Infof("OnDelete started %s %s/%s", s.metaKey, topoObject.GetNamespace(), topoObject.GetName())

	cluster := getObjectCluster(topoObject)
	node := s.getNode(cluster, topoObject.GetNamespace(), topoObject.GetName())
	if node == nil {
		return
	}

	deleteAllRelation(node)
	node.checkGC()

	s.manager.newNodeEvent(node, EventTypeDelete)
	klog.V(6).Infof("OnDelete finished %s %s/%s", s.metaKey, topoObject.GetNamespace(), topoObject.GetName())
}

func (s *nodeStorage) addNode(obj Object, node *nodeInfo) {
	node.updateNodeMeta(obj)
	if len(node.labelRelations) != 0 {
		klog.Warningf("unexpected labelRelations {%v}", node.labelRelations)
		node.labelRelations = nil
	}
	node.relationsLock.Lock()
	for _, resolver := range s.resolvers {
		relations := resolver.Resolve(obj)
		for _, relation := range relations {
			if relation.LabelSelector != nil {
				node.labelRelations = append(node.labelRelations, relation)
			} else {
				s.addDirectResourceRelation(node, &relation)
			}
		}
	}
	slices.SortFunc(node.labelRelations, compareLabelResourceRelation)
	for _, relation := range node.labelRelations {
		s.addLabelResourceRelation(node, &relation)
	}
	node.relationsLock.Unlock()

	for _, discoverer := range s.discoverers {
		preStorage := s.manager.getStorage(discoverer.PreMeta)
		preObjs := discoverer.Discover(obj)
		for _, preObj := range preObjs {
			preNode := preStorage.getOrCreateNode(node.cluster, preObj.Namespace, preObj.Name)
			rangeAndSetDirectRefRelation(preNode, node, s.manager)
		}
	}

	for _, preOrderStorage := range s.preOrderResources {
		preOrderStorage.checkForLabelUpdate(node)
	}
}

func (s *nodeStorage) addLabelResourceRelation(node *nodeInfo, relation *ResourceRelation) {
	postMeta := relation.PostMeta
	postMetaKey := generateMetaKey(postMeta)
	postStorage := s.manager.getStorage(postMeta)
	if postStorage == nil {
		klog.Errorf("Failed to get node storage by meta %s, ignore this relation", postMetaKey)
		return
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

func (s *nodeStorage) setDirectResourceRelation(node *nodeInfo, directRefs map[nodeMeta]interface{}) {
	var toDel []*nodeInfo
	rangeNodeList(node.directReferredPostOrders, func(postNode *nodeInfo) {
		postNodeMeta := nodeMeta{
			apiVersion: postNode.storageRef.meta.APIVersion,
			kind:       postNode.storageRef.meta.Kind,
			cluster:    postNode.cluster,
			namespace:  postNode.namespace,
			name:       postNode.name,
		}
		if _, ok := directRefs[postNodeMeta]; !ok {
			toDel = append(toDel, postNode)
		} else {
			delete(directRefs, postNodeMeta)
		}
	})

	for _, postNode := range toDel {
		if deleteDirectRelation(node, postNode) {
			node.noticePostOrderRelationDeleted(postNode)
			postNode.checkGC()
		}
	}

	for postNodeMeta := range directRefs {
		postStorage := s.manager.getStorage(metav1.TypeMeta{APIVersion: postNodeMeta.apiVersion, Kind: postNodeMeta.kind})
		if postStorage == nil {
			klog.Errorf("Failed to get node storage by meta %s, ignore this relation", generateKey(postNodeMeta.apiVersion, postNodeMeta.kind))
			continue
		}
		postNode := postStorage.getOrCreateNode(postNodeMeta.cluster, postNodeMeta.namespace, postNodeMeta.name)
		rangeAndSetDirectRefRelation(node, postNode, s.manager)
	}
}

func (s *nodeStorage) addDirectResourceRelation(node *nodeInfo, relation *ResourceRelation) {
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
}

func (s *nodeStorage) removeLabelResourceRelation(node *nodeInfo, relation *ResourceRelation) {
	postStorage := s.manager.getStorage(relation.PostMeta)
	if postStorage == nil {
		klog.Error("Failed to get node Storage by %s, ignore this delete request",
			generateMetaKey(relation.PostMeta))
		return
	}

	if relation.LabelSelector != nil {
		postNodes := postStorage.getMatchedNodeList(node.cluster, node.namespace, relation.LabelSelector)
		for _, postNode := range postNodes {
			if deleteLabelRelation(node, postNode) {
				node.noticePostOrderRelationDeleted(postNode)
			}
		}
	}
}
