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
	"time"

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
		e, ok := item.(nodeEvent)
		if !ok {
			klog.Errorf("Unexpected node event queue item %v", item)
			continue
		}
		storage := e.node.storageRef
		if storage == nil {
			klog.Errorf("Unexpected nil nodeStorage for nodeEvent node %v", e.node)
			continue
		}

		switch e.eventType {
		case EventTypeAdd:
			for _, h := range storage.nodeUpdateHandler {
				h.OnAdd(e.node)
			}
		case EventTypeUpdate:
			for _, h := range storage.nodeUpdateHandler {
				h.OnUpdate(e.node)
			}
		case EventTypeDelete:
			for _, h := range storage.nodeUpdateHandler {
				h.OnDelete(e.node)
			}
		case EventTypeRelatedUpdate:
			for _, h := range storage.nodeUpdateHandler {
				h.OnRelatedUpdate(e.node)
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
	m.nodeEventQueue.Add(nodeEvent{
		eventType: eType,
		node:      info,
	})
}

func (m *manager) newRelationEvent(preNode, postNode *nodeInfo, eType eventType) {
	m.relationEventQueue <- relationEvent{
		eventType: eType,
		preNode:   preNode,
		postNode:  postNode,
	}
}
