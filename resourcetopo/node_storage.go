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
	"fmt"
	"sync"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
)

var _ TopoNodeStorage = &nodeStorage{}

// nodeStorage is an implementation of TopoNodeStorage.
// It's the entrance of all nodes of same meta resource type and related config.
type nodeStorage struct {
	manager *manager

	meta            metav1.TypeMeta // meta is the resource type this nodeStorage handle
	metaKey         string          // metakey is generated key string from meta
	virtualResource bool            // virtualResource means whether this type resource is virtual or not

	preOrderResources  map[string]*nodeStorage // metaKey => corresponding nodeStorage reference
	postNoticeRelation map[string]interface{}  // set for configured reverse-notice relation of post resources metaKey
	ownerRelation      map[string]interface{}  // set for configured owner relation of post resources metaKey

	resolvers   []RelationResolver          // resolvers list whose PreMeta is of this type
	discoverers []VirtualResourceDiscoverer // discovers list whose PostMeta is of this type

	handlersLock          sync.Mutex                   // lock to protect handlers update operations
	relationUpdateHandler map[string][]RelationHandler // post-order metaKey => RelationHandler list
	nodeUpdateHandler     []NodeHandler                // NodeHandler list

	storageLock    sync.RWMutex                    // lock to protect namespacedInfo CRUD operations
	namespacedInfo map[string]map[string]*nodeInfo // namespace => name => object info
}

func newNodeStorage(manager *manager, informer Informer, meta metav1.TypeMeta) *nodeStorage {
	s := &nodeStorage{
		manager:            manager,
		meta:               meta,
		metaKey:            generateMetaKey(meta),
		namespacedInfo:     make(map[string]map[string]*nodeInfo),
		postNoticeRelation: make(map[string]interface{}),
		ownerRelation:      make(map[string]interface{}),
	}

	informer.AddEventHandler(s)
	return s
}

func newVirtualStorage(manager *manager, meta metav1.TypeMeta) *nodeStorage {
	return &nodeStorage{
		manager:         manager,
		meta:            meta,
		metaKey:         generateMetaKey(meta),
		virtualResource: true,
		namespacedInfo:  make(map[string]map[string]*nodeInfo),
	}
}

// GetNode return the ref to Node that match the node's name.
func (s *nodeStorage) GetNode(namespacedName types.NamespacedName) (NodeInfo, error) {
	node := s.getNode(namespacedName.Namespace, namespacedName.Name)
	if node != nil && !node.objectExisted {
		return nil, nil
	}
	return node, nil
}

func (s *nodeStorage) addNodeHandler(handler NodeHandler) {
	s.handlersLock.Lock()
	defer s.handlersLock.Unlock()

	s.nodeUpdateHandler = append(s.nodeUpdateHandler, handler)
}

func (s *nodeStorage) addRelationHandler(postMeta metav1.TypeMeta, relationHandler RelationHandler) {
	s.handlersLock.Lock()
	defer s.handlersLock.Unlock()
	if s.relationUpdateHandler == nil {
		s.relationUpdateHandler = make(map[string][]RelationHandler)
	}
	postKey := generateMetaKey(postMeta)

	s.relationUpdateHandler[postKey] = append(s.relationUpdateHandler[postKey], relationHandler)
}

func (s *nodeStorage) addRelationConfig(r *RelationResolver) error {
	for _, p := range r.PostMetas {
		postStorage := s.manager.getStorage(p)
		if postStorage == nil {
			return fmt.Errorf("failed to get storage with meta %v", p)
		}
		postStorage.addPreOrder(s.meta)
	}
	s.resolvers = append(s.resolvers, *r)

	if len(r.ReverseNotice) > 0 {
		for _, meta := range r.ReverseNotice {
			s.postNoticeRelation[generateMetaKey(meta)] = nil
		}
	}
	if len(r.OwnerRelation) > 0 {
		for _, meta := range r.OwnerRelation {
			s.ownerRelation[generateMetaKey(meta)] = nil
		}
	}

	return nil
}

func (s *nodeStorage) addDiscoverConfig(d *VirtualResourceDiscoverer) error {
	s.addPreOrder(d.PreMeta)
	s.discoverers = append(s.discoverers, *d)
	return nil
}

func (s *nodeStorage) addPreOrder(preOrder metav1.TypeMeta) {
	if s.preOrderResources == nil {
		s.preOrderResources = make(map[string]*nodeStorage)
	}
	key := generateMetaKey(preOrder)
	preStorage := s.manager.getStorage(preOrder)
	if preStorage != nil {
		s.preOrderResources[key] = preStorage
	}
}

func (s *nodeStorage) getOrCreateNode(namespace, name string) *nodeInfo {
	if node := s.getNode(namespace, name); node != nil {
		return node
	}
	return s.createNode(namespace, name)
}

func (s *nodeStorage) getNode(namespace, name string) *nodeInfo {
	namespace = getNamespacedKey(namespace)
	s.storageLock.RLock()
	defer s.storageLock.RUnlock()

	namespcedInfo := s.namespacedInfo[namespace]
	if namespcedInfo != nil {
		return namespcedInfo[name]
	} else {
		return nil
	}
}

func (s *nodeStorage) createNode(namespace, name string) *nodeInfo {
	node := newNode(s, namespace, name)
	namespace = getNamespacedKey(namespace)
	s.storageLock.Lock()
	defer s.storageLock.Unlock()

	if s.namespacedInfo[namespace] == nil {
		s.namespacedInfo[namespace] = make(map[string]*nodeInfo)
	}
	s.namespacedInfo[namespace][name] = node

	if s.virtualResource {
		s.manager.newNodeEvent(node, EventTypeAdd)
	}

	return node
}

func (s *nodeStorage) deleteNode(namespace, name string) {
	namespace = getNamespacedKey(namespace)
	s.storageLock.Lock()
	defer s.storageLock.Unlock()

	namedInfo := s.namespacedInfo[namespace]
	if namedInfo != nil {
		delete(namedInfo, name)
	}
}

func (s *nodeStorage) getMatchedNodeListWithOwner(namespace string, labelSelector *metav1.LabelSelector, owner *nodeInfo) []*nodeInfo {
	selector, err := metav1.LabelSelectorAsSelector(labelSelector)
	if err != nil {
		klog.Errorf("Failed to resolve labelSelector %v: %s", labelSelector, err.Error())
		return nil
	}

	var res []*nodeInfo
	appendFunc := func(info *nodeInfo) {
		for _, nodeOwner := range info.ownerNodes {
			if nodeOwner.metaKey == owner.storageRef.metaKey &&
				nodeOwner.name == owner.name {
				res = append(res, info)
				return
			}
		}
	}

	// for owner relation resource, k8s promise they are in same namespace or both clusterScoped
	namespace = getNamespacedKey(namespace)
	s.storageLock.RLock()
	defer s.storageLock.RUnlock()

	getMatchedNodeList(selector, s.namespacedInfo[namespace], appendFunc)

	return res
}

func (s *nodeStorage) getMatchedNodeList(namespace string, labelSelector *metav1.LabelSelector) []*nodeInfo {
	selector, err := metav1.LabelSelectorAsSelector(labelSelector)
	if err != nil {
		klog.Errorf("Failed to resolve labelSelector %v: %s", labelSelector, err.Error())
		return nil
	}

	var res []*nodeInfo
	appendFunc := func(info *nodeInfo) {
		res = append(res, info)
	}

	s.storageLock.RLock()
	defer s.storageLock.RUnlock()

	if !isClusterNamespace(namespace) {
		getMatchedNodeList(selector, s.namespacedInfo[namespace], appendFunc)
		return res
	}
	for _, v := range s.namespacedInfo {
		getMatchedNodeList(selector, v, appendFunc)
	}
	return res
}

// checkForLabelUpdate called when postNode is newly added into graph,
// call this to check any pre node need to add new relation
func (s *nodeStorage) checkForLabelUpdate(postNode *nodeInfo) {
	ns := getNamespacedKey(postNode.namespace)
	s.storageLock.RLock()
	defer s.storageLock.RUnlock()

	nodeList := s.namespacedInfo[ns]
	for _, n := range nodeList {
		if len(n.relations) == 0 {
			continue
		}
		for _, relation := range n.relations {
			if !typeEqual(relation.PostMeta, postNode.storageRef.meta) {
				continue
			}
			selector, err := metav1.LabelSelectorAsSelector(relation.LabelSelector)
			if err != nil {
				klog.Errorf("Failed to resolve selector %v: %s", relation.LabelSelector, err.Error())
				continue
			}
			if postNode.matched(selector) {
				if _, ok := s.ownerRelation[postNode.storageRef.metaKey]; ok && !postNode.ownerMatched(n) {
					continue
				}
				rangeAndSetLabelRelation(n, postNode, s.manager)
			} else {
				if deleteLabelRelation(n, postNode) {
					n.postOrderRelationDeleted(postNode)
				}
			}
		}
	}
}

func getMatchedNodeList(selector labels.Selector, m map[string]*nodeInfo, appendFunc func(info *nodeInfo)) {
	for _, n := range m {
		if n.matched(selector) {
			appendFunc(n)
		}
	}
}

const allNamespaceKey = "_all_namespaces"

func getNamespacedKey(namespace string) string {
	if len(namespace) == 0 {
		return allNamespaceKey
	} else {
		return namespace
	}
}

func isClusterNamespace(namespace string) bool {
	return len(namespace) == 0
}
