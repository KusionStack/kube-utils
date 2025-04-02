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
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog/v2"
)

type eventType string

const (
	EventTypeAdd           eventType = "add"
	EventTypeUpdate        eventType = "update"
	EventTypeDelete        eventType = "delete"
	EventTypeRelatedUpdate eventType = "relatedUpdate"
)

const (
	defaultNodeEventHandleRateMinDelay     = time.Millisecond
	defaultNodeEventHandleRateMaxDelay     = time.Second
	defaultRelationEventHandleRateMinDelay = time.Millisecond
	defaultRelationEventHandleRateMaxDelay = time.Second
	defaultNodeEventHandlePeriod           = time.Second
	defaultRelationEventHandlePeriod       = time.Second
)

func (m *manager) startHandleEvent(stopCh <-chan struct{}) {
	go wait.Until(m.handleNodeEvent, defaultNodeEventHandlePeriod, stopCh)
	go wait.Until(m.handleRelationEvent, defaultRelationEventHandlePeriod, stopCh)
}

func (m *manager) handleNodeEvent() {
	for {
		item, shutdown := m.nodeEventQueue.Get()
		if shutdown {
			return
		}
		info, ok := item.(string)
		if !ok {
			klog.Errorf("Unexpected node event queue item %v", item)
			continue
		}
		evtType, node := m.decodeString2NodeEvent(info)
		if node == nil {
			continue
		}

		storage := node.storageRef
		if storage == nil {
			klog.Errorf("Unexpected nil nodeStorage for nodeEvent node %v", node)
			continue
		}

		switch evtType {
		case EventTypeAdd:
			for _, h := range storage.nodeUpdateHandler {
				h.OnAdd(node)
			}
		case EventTypeUpdate:
			for _, h := range storage.nodeUpdateHandler {
				h.OnUpdate(node)
			}
		case EventTypeDelete:
			for _, h := range storage.nodeUpdateHandler {
				h.OnDelete(node)
			}
		case EventTypeRelatedUpdate:
			for _, h := range storage.nodeUpdateHandler {
				h.OnRelatedUpdate(node)
			}
		}
		m.nodeEventQueue.Done(item)
	}
}

func (m *manager) handleRelationEvent() {
	for {
		item, shutdown := m.relationEventQueue.Get()
		if shutdown {
			return
		}

		info, ok := item.(string)
		if !ok {
			klog.Errorf("Unexpected relation event queue item %v", item)
			continue
		}
		evtType, preNode, postNode := m.decodeString2RelationEvent(info)
		if preNode == nil || postNode == nil {
			continue
		}

		storage := preNode.storageRef
		if storage == nil {
			klog.Errorf("Unexpected nil nodeStorage for relaltion event preNode node %v", preNode)
			continue
		}
		handlers := storage.relationUpdateHandler[postNode.storageRef.metaKey]
		if handlers == nil {
			continue
		}

		switch evtType {
		case EventTypeAdd:
			for _, handler := range handlers {
				handler.OnAdd(preNode, postNode)
			}
		case EventTypeDelete:
			for _, handler := range handlers {
				handler.OnDelete(preNode, postNode)
			}
		}
		m.relationEventQueue.Done(item)
	}
}

func (m *manager) newNodeEvent(info *nodeInfo, eType eventType) {
	key := m.encodeNodeEvent2String(eType, info)
	m.nodeEventQueue.AddRateLimited(key)
}

func (m *manager) newRelationEvent(preNode, postNode *nodeInfo, eType eventType) {
	key := m.encodeRelationEvent2String(eType, preNode, postNode)
	m.relationEventQueue.AddRateLimited(key)
}

const keySpliter = "%"

func (m *manager) encodeNodeEvent2String(eType eventType, node *nodeInfo) string {
	return strings.Join([]string{
		node.storageRef.meta.APIVersion,
		node.storageRef.meta.Kind,
		node.cluster,
		node.namespace,
		node.name,
		string(eType),
	}, keySpliter)
}

func (m *manager) decodeString2NodeEvent(key string) (eventType, *nodeInfo) {
	info := strings.Split(key, keySpliter)
	if len(info) != 6 {
		klog.Errorf("Unexpected event key %v", key)
		return "", nil
	}

	node := m.getOrCreateMockNode(info[0], info[1], info[2], info[3], info[4])
	return eventType(info[5]), node
}

func (m *manager) encodeRelationEvent2String(eType eventType, preNode, postNode *nodeInfo) string {
	return strings.Join([]string{
		preNode.storageRef.meta.APIVersion,
		preNode.storageRef.meta.Kind,
		preNode.cluster, preNode.namespace, preNode.name,
		postNode.storageRef.meta.APIVersion,
		postNode.storageRef.meta.Kind,
		postNode.cluster, postNode.namespace, postNode.name,
		string(eType),
	}, keySpliter)
}

func (m *manager) decodeString2RelationEvent(key string) (eventType, *nodeInfo, *nodeInfo) {
	info := strings.Split(key, keySpliter)
	if len(info) != 11 {
		klog.Errorf("Unexpected relation event key %v", key)
		return "", nil, nil
	}
	preNode := m.getOrCreateMockNode(info[0], info[1], info[2], info[3], info[4])
	postNode := m.getOrCreateMockNode(info[5], info[6], info[7], info[8], info[9])

	return eventType(info[10]), preNode, postNode
}

func (m *manager) getOrCreateMockNode(apiVersion, kind, cluster, namespace, name string) *nodeInfo {
	s := m.getStorage(metav1.TypeMeta{
		APIVersion: apiVersion,
		Kind:       kind,
	})
	if s == nil {
		klog.Errorf("Unexpected nil nodeStorage for %s %s", apiVersion, kind)
		return nil
	}
	node := s.getNode(cluster, namespace, name)
	if node == nil {
		// node may be deleted, reconstruct it for event handle
		node = &nodeInfo{
			storageRef: s,
			cluster:    cluster,
			namespace:  namespace,
			name:       name,
		}
	}
	return node
}
