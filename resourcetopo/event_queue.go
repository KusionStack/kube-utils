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
	defaultNodeEventQueueSize        = 1000
	defaultRelationEventQueueSize    = 1000
	defaultNodeEventHandlePeriod     = time.Second
	defaultRelationEventHandlePeriod = time.Second
)

type nodeEvent struct {
	eventType eventType
	node      *nodeInfo
}

type relationEvent struct {
	eventType eventType
	preNode   *nodeInfo
	postNode  *nodeInfo
}

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
		eventType, node := m.decodeString2Event(info)
		if node == nil {
			continue
		}

		storage := node.storageRef
		if storage == nil {
			klog.Errorf("Unexpected nil nodeStorage for nodeEvent node %v", node)
			continue
		}

		switch eventType {
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
		select {
		case e := <-m.relationEventQueue:
			storage := e.preNode.storageRef
			if storage == nil {
				klog.Errorf("Unexpected nil nodeStorage for relaltion event preNode node %v", e.preNode)
				continue
			}
			handlers := storage.relationUpdateHandler[e.postNode.storageRef.metaKey]
			if handlers == nil {
				continue
			}

			switch e.eventType {
			case EventTypeAdd:
				for _, handler := range handlers {
					handler.OnAdd(e.preNode, e.postNode)
				}
			case EventTypeDelete:
				for _, handler := range handlers {
					handler.OnDelete(e.preNode, e.postNode)
				}
			}
		default:
			break
		}
	}
}

func (m *manager) newNodeEvent(info *nodeInfo, eType eventType) {
	key := m.encodeEvent2String(eType, info)
	m.nodeEventQueue.AddRateLimited(key)
}

func (m *manager) newRelationEvent(preNode, postNode *nodeInfo, eType eventType) {
	m.relationEventQueue <- relationEvent{
		eventType: eType,
		preNode:   preNode,
		postNode:  postNode,
	}
}

const keySpliter = "%"

func (m *manager) encodeEvent2String(eType eventType, node *nodeInfo) string {
	return strings.Join([]string{
		node.storageRef.meta.APIVersion,
		node.storageRef.meta.Kind,
		node.cluster,
		node.namespace,
		node.name,
		string(eType),
	}, keySpliter)
}

func (m *manager) decodeString2Event(key string) (eventType, *nodeInfo) {
	info := strings.Split(key, keySpliter)
	s := m.getStorage(metav1.TypeMeta{
		APIVersion: info[0],
		Kind:       info[1],
	})
	if s == nil {
		klog.Errorf("Unexpected nil nodeStorage for event %v", key)
		return "", nil
	}
	node := s.getNode(info[2], info[3], info[4])
	if node == nil {
		// node may be deleted, reconstruct it for event handle
		node = &nodeInfo{
			storageRef: s,
			cluster:    info[2],
			namespace:  info[3],
			name:       info[4],
		}
	}
	return eventType(info[5]), node
}
