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
	"context"
	"strings"
	"sync/atomic"
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
	defaultEventHandlerTimeout             = time.Second
)

func (m *manager) startHandleEvent(stopCh <-chan struct{}) {
	go wait.Until(m.handleNodeEvent, defaultNodeEventHandlePeriod, stopCh)
	go wait.Until(m.handleRelationEvent, defaultRelationEventHandlePeriod, stopCh)
}

func (m *manager) handleNodeEvent() {
	for {
		item, shutdown := m.nodeEventQueue.Get()
		klog.V(6).Infof("handleNodeEvent: item: %v", item)
		if shutdown {
			return
		}
		info, ok := item.(string)
		if !ok {
			klog.Errorf("Unexpected node event queue item %v", item)
			m.nodeEventQueue.Done(item)
			continue
		}
		evtType, node := m.decodeString2NodeEvent(info)
		if node == nil {
			m.nodeEventQueue.Done(item)
			continue
		}

		storage := node.storageRef
		if storage == nil {
			klog.Errorf("Unexpected nil nodeStorage for nodeEvent node %v", node)
			m.nodeEventQueue.Done(item)
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), m.eventHandlerTimeout)
		nodeMetrics := &execMetrics{
			timeouts: &m.nodeHandlerTimeouts,
			panics:   &m.nodeHandlerPanics,
			active:   &m.activeHandlerGoroutines,
		}
		switch evtType {
		case EventTypeAdd:
			executeHandlers(ctx, storage.nodeUpdateHandler, func(h NodeHandler) {
				h.OnAdd(ctx, node)
			}, "Node handler", func() {
				klog.Warningf("Node handler OnAdd timeout for node %s/%s, skipping remaining handlers", node.namespace, node.name)
			}, nodeMetrics)
		case EventTypeUpdate:
			executeHandlers(ctx, storage.nodeUpdateHandler, func(h NodeHandler) {
				h.OnUpdate(ctx, node)
			}, "Node handler", func() {
				klog.Warningf("Node handler OnUpdate timeout for node %s/%s, skipping remaining handlers", node.namespace, node.name)
			}, nodeMetrics)
		case EventTypeDelete:
			executeHandlers(ctx, storage.nodeUpdateHandler, func(h NodeHandler) {
				h.OnDelete(ctx, node)
			}, "Node handler", func() {
				klog.Warningf("Node handler OnDelete timeout for node %s/%s, skipping remaining handlers", node.namespace, node.name)
			}, nodeMetrics)
		case EventTypeRelatedUpdate:
			executeHandlers(ctx, storage.nodeUpdateHandler, func(h NodeHandler) {
				h.OnRelatedUpdate(ctx, node)
			}, "Node handler", func() {
				klog.Warningf("Node handler OnRelatedUpdate timeout for node %s/%s, skipping remaining handlers", node.namespace, node.name)
			}, nodeMetrics)
		}
		cancel()
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
			m.relationEventQueue.Done(item)
			continue
		}
		evtType, preNode, postNode := m.decodeString2RelationEvent(info)
		if preNode == nil || postNode == nil {
			m.relationEventQueue.Done(item)
			continue
		}

		storage := preNode.storageRef
		if storage == nil {
			klog.Errorf("Unexpected nil nodeStorage for relaltion event preNode node %v", preNode)
			m.relationEventQueue.Done(item)
			continue
		}
		handlers := storage.relationUpdateHandler[postNode.storageRef.metaKey]
		if handlers == nil {
			m.relationEventQueue.Done(item)
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), m.eventHandlerTimeout)
		relationMetrics := &execMetrics{
			timeouts: &m.relationHandlerTimeouts,
			panics:   &m.relationHandlerPanics,
			active:   &m.activeHandlerGoroutines,
		}
		switch evtType {
		case EventTypeAdd:
			executeHandlers(ctx, handlers, func(h RelationHandler) {
				h.OnAdd(ctx, preNode, postNode)
			}, "Relation handler", func() {
				klog.Warningf("Relation handler OnAdd timeout for relation %s/%s -> %s/%s, skipping remaining handlers",
					preNode.namespace, preNode.name, postNode.namespace, postNode.name)
			}, relationMetrics)
		case EventTypeDelete:
			executeHandlers(ctx, handlers, func(h RelationHandler) {
				h.OnDelete(ctx, preNode, postNode)
			}, "Relation handler", func() {
				klog.Warningf("Relation handler OnDelete timeout for relation %s/%s -> %s/%s, skipping remaining handlers",
					preNode.namespace, preNode.name, postNode.namespace, postNode.name)
			}, relationMetrics)
		}
		cancel()
		m.relationEventQueue.Done(item)
	}
}

// execMetrics holds atomic counters updated by executeHandlers.
type execMetrics struct {
	timeouts, panics, active *atomic.Int64
}

// executeHandlers executes handlers with timeout support using generics.
// Each handler runs in its own goroutine. If the context deadline fires before
// a handler completes, remaining handlers are skipped and onTimeout is called.
func executeHandlers[H any](ctx context.Context, handlers []H, exec func(H), panicMsg string, onTimeout func(), m *execMetrics) {
	timeout := false
	for _, h := range handlers {
		if timeout {
			break
		}
		done := make(chan struct{})
		m.active.Add(1)
		go func(handler H) {
			defer func() {
				m.active.Add(-1)
				if r := recover(); r != nil {
					klog.Errorf("%s panicked: %v, treating as failed", panicMsg, r)
					m.panics.Add(1)
				}
				close(done)
			}()
			exec(handler)
		}(h)
		select {
		case <-done:
			// Handler completed
		case <-ctx.Done():
			timeout = true
			m.timeouts.Add(1)
			onTimeout()
		}
	}
}

func (m *manager) newNodeEvent(info *nodeInfo, eType eventType) {
	key := m.encodeNodeEvent2String(eType, info)
	klog.V(6).Infof("New node event queue key: %v", key)
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
