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
	"fmt"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
)

var _ Manager = &manager{}

type manager struct {
	relationEventQueue chan relationEvent
	nodeEventQueue     workqueue.RateLimitingInterface
	configLock         sync.Mutex
	started            bool

	storages map[string]*nodeStorage // meta => nodeStorage and status info
}

func NewResourcesTopoManager(cfg ManagerConfig) (Manager, error) {
	checkManagerConfig(&cfg)
	ratelimiter := workqueue.NewItemExponentialFailureRateLimiter(1*time.Millisecond, 1*time.Second)

	m := &manager{
		nodeEventQueue:     workqueue.NewNamedRateLimitingQueue(ratelimiter, "resourcetopoNodeEventQueue"),
		relationEventQueue: make(chan relationEvent, cfg.RelationEventQueueSize),
		storages:           make(map[string]*nodeStorage),
	}

	if cfg.TopologyConfig != nil {
		if err := m.AddTopologyConfig(*cfg.TopologyConfig); err != nil {
			return nil, err
		}
	}
	return m, nil
}

// AddTopologyConfig add new resource topology config to this manager
func (m *manager) AddTopologyConfig(cfg TopologyConfig) error {
	m.configLock.Lock()
	defer m.configLock.Unlock()

	for _, r := range cfg.Resolvers {
		preOrderStorage, err := m.getOrCreateStorage(r.PreMeta, cfg.GetInformer)
		if err != nil {
			return err
		}
		for _, v := range r.PostMetas {
			_, err := m.getOrCreateStorage(v, cfg.GetInformer)
			if err != nil {
				return err
			}
		}
		if err = preOrderStorage.addRelationConfig(&r); err != nil {
			return err
		}
	}

	for _, d := range cfg.Discoverers {
		s, err := m.getOrCreateStorage(d.PostMeta, cfg.GetInformer)
		if err != nil {
			return err
		}
		_, err = m.createVirtualStorage(d.PreMeta)
		if err != nil {
			return err
		}

		if err = s.addDiscoverConfig(&d); err != nil {
			return err
		}
	}

	if err := m.dagCheck(); err != nil {
		klog.Errorf("Failed to check resource topology config: %s", err.Error())
		return err
	}

	return nil
}

// AddRelationHandler add a new relation handler for the relation change between
// the two type nodes configured by preMeta and postMeta
func (m *manager) AddRelationHandler(preOrder, postOrder metav1.TypeMeta, handler RelationHandler) error {
	preOrderKey := generateMetaKey(preOrder)
	preStorage, ok := m.storages[preOrderKey]
	if !ok {
		return fmt.Errorf("failed to find initialized nodeStorage for resource %s", preOrderKey)
	}
	preStorage.addRelationHandler(postOrder, handler)
	return nil
}

// AddNodeHandler add a new node handler for nodes of type configured by meta.
func (m *manager) AddNodeHandler(meta metav1.TypeMeta, handler NodeHandler) error {
	s := m.storages[generateMetaKey(meta)]
	if s == nil {
		return fmt.Errorf("resource %v not configured in this manager", meta)
	}
	s.addNodeHandler(handler)
	return nil
}

// Start to handler relation events and node events.
// Start can only be called once.
func (m *manager) Start(stopCh <-chan struct{}) {
	m.configLock.Lock()
	defer m.configLock.Unlock()
	if !m.started {
		m.startHandleEvent(stopCh)
		m.started = true
	} else {
		klog.Warning("resourcetopo Manager has already started, ignore this call.")
	}
}

// GetTopoNodeStorage return the ref to TopoNodeStorage that match resource meta.
func (m *manager) GetTopoNodeStorage(meta metav1.TypeMeta) (TopoNodeStorage, error) {
	if s := m.storages[generateMetaKey(meta)]; s == nil {
		return nil, fmt.Errorf("resource %v not configured in this manager", meta)
	} else {
		return s, nil
	}
}

// GetNode return the ref to Node that match the resouorce meta and node's name if existed.
func (m *manager) GetNode(meta metav1.TypeMeta, name types.NamespacedName) (NodeInfo, error) {
	s, err := m.GetTopoNodeStorage(meta)
	if err != nil {
		return nil, err
	}
	return s.GetNode(name)
}

func (m *manager) getOrCreateStorage(typeMeta metav1.TypeMeta, getInformer func(meta metav1.TypeMeta) Informer) (*nodeStorage, error) {
	if getInformer == nil {
		return nil, fmt.Errorf("unexpected nil getInformer func")
	}
	key := generateMetaKey(typeMeta)

	s, ok := m.storages[key]
	if ok {
		return s, nil
	}

	informer := getInformer(typeMeta)
	if informer == nil {
		return nil, fmt.Errorf("failed to get informer for resource %s", key)
	}

	s = newNodeStorage(m, informer, typeMeta)
	m.storages[key] = s
	return s, nil
}

func (m *manager) getStorage(meta metav1.TypeMeta) *nodeStorage {
	key := generateMetaKey(meta)

	return m.storages[key]
}

func (m *manager) createVirtualStorage(meta metav1.TypeMeta) (*nodeStorage, error) {
	key := generateMetaKey(meta)
	_, ok := m.storages[key]
	if ok {
		return nil, fmt.Errorf("unexpected twice configuration for virtual resource %s", key)
	}

	s := newVirtualStorage(m, meta)
	m.storages[key] = s
	return s, nil
}

// dagCheck implement a depth first algorithm to make sure the resource topology in this manager has no cycle
func (m *manager) dagCheck() error {
	visited := make(map[string]bool)
	stack := list.New()

	for k, s := range m.storages {
		if visited[k] {
			if existInList(stack, k) {
				return fmt.Errorf("DAG check for resource %s failed", k)
			} else {
				continue
			}
		}
		if err := checkNode(s, stack, visited); err != nil {
			return err
		}
	}
	return nil
}

func checkManagerConfig(c *ManagerConfig) {
	if c.NodeEventQueueSize <= 0 {
		c.NodeEventQueueSize = defaultNodeEventQueueSize
	}
	if c.RelationEventQueueSize <= 0 {
		c.RelationEventQueueSize = defaultRelationEventQueueSize
	}
}
