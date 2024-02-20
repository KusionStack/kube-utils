/**
 * Copyright 2023 KusionStack Authors.
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

package controller

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	"kusionstack.io/kube-utils/multicluster/metrics"
)

type ClusterProvider interface {
	Init(config *rest.Config)                                     // Init is used to initialize the cluster provider, config is the kubeconfig for the fed cluster
	GetClusterMangementGVR() schema.GroupVersionResource          // The GVR will be used to watch cluster management resource
	GetClusterName(obj *unstructured.Unstructured) string         // Get cluster name from cluster management resource, cluster name is used to identify the cluster
	GetClusterConfig(obj *unstructured.Unstructured) *rest.Config // Get kubeconfig from cluster management resource
}

type Controller struct {
	config          *rest.Config
	clusterProvider ClusterProvider

	client          dynamic.Interface // Client to get cluster info
	informerFactory dynamicinformer.DynamicSharedInformerFactory
	informer        cache.SharedIndexInformer
	workqueue       workqueue.RateLimitingInterface

	mutex     sync.RWMutex
	syncedNum int           // Number of synced cluster
	syncedCh  chan struct{} // Channel to notify all synced clusters have been processed

	addUpdateHandler func(string) error // When cluster is added or updated, this handler will be invoked
	deleteHandler    func(string)       // When cluster is deleted, this handler will be invoked

	clusterNameToNamespacedKey map[string]string
	namespacedKeyToObj         map[string]*unstructured.Unstructured
	log                        logr.Logger
}

type ControllerConfig struct {
	Config          *rest.Config // Kubeconfig for the fed cluster
	ClusterProvider ClusterProvider
	ResyncPeriod    time.Duration // Resync period for cluster management
	Log             logr.Logger
}

// NewController creates a new Controller which will process events about cluster.
func NewController(cfg *ControllerConfig) (*Controller, error) {
	client, err := dynamic.NewForConfig(cfg.Config)
	if err != nil {
		return nil, err
	}
	if cfg.ClusterProvider == nil {
		return nil, fmt.Errorf("ClusterProvider is required")
	}

	informerFactory := dynamicinformer.NewDynamicSharedInformerFactory(client, cfg.ResyncPeriod)
	informer := informerFactory.ForResource(cfg.ClusterProvider.GetClusterMangementGVR()).Informer()

	return &Controller{
		config:          cfg.Config,
		clusterProvider: cfg.ClusterProvider,

		client:          client,
		informerFactory: informerFactory,
		informer:        informer,
		workqueue:       workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), cfg.ClusterProvider.GetClusterMangementGVR().Resource),
		syncedCh:        make(chan struct{}),

		clusterNameToNamespacedKey: make(map[string]string),                     // Get namespaced key by cluster name
		namespacedKeyToObj:         make(map[string]*unstructured.Unstructured), // Get cluster management resource by namespaced key
		log:                        cfg.Log,
	}, nil
}

// AddEventHandler adds handlers which will be invoked.
// When cluster is added or updated, addUpdateHandler will be invoked.
// When cluster is deleted, deleteHandler will be invoked.
func (c *Controller) AddEventHandler(addUpdateHandler func(string) error, deleteHandler func(string)) {
	c.informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: c.enqueueClusterEvent,
		UpdateFunc: func(old, new interface{}) {
			c.enqueueClusterEvent(new)
		},
		DeleteFunc: c.enqueueClusterEvent,
	})

	c.addUpdateHandler = addUpdateHandler
	c.deleteHandler = deleteHandler
}

func (c *Controller) Run(threadiness int, stopCh <-chan struct{}) error {
	defer runtime.HandleCrash()
	defer c.workqueue.ShutDown()

	c.clusterProvider.Init(c.config)

	c.informerFactory.Start(stopCh)

	// Wait for the caches to be synced before starting workers
	if ok := cache.WaitForCacheSync(stopCh, c.informer.HasSynced); !ok {
		return fmt.Errorf("failed to wait for caches to sync")
	}

	c.mutex.Lock()
	c.syncedNum = c.workqueue.Len()
	c.mutex.Unlock()

	// Start workers to process cluster events
	for i := 0; i < threadiness; i++ {
		go wait.Until(c.runWorker, time.Second, stopCh)
	}

	<-stopCh
	return nil
}

func (c *Controller) WaitForSynced(ctx context.Context) bool {
	select {
	case <-c.syncedCh: // Wait for all cluster has been processed
		return true
	case <-ctx.Done():
		return false
	}
}

func (c *Controller) enqueueClusterEvent(obj interface{}) {
	key, err := cache.MetaNamespaceKeyFunc(obj)
	if err != nil {
		c.log.Error(err, "failed to get enqueue key")
		return
	}
	c.workqueue.Add(key)
}

func (c *Controller) runWorker() {
	for c.processNextWorkItem() {
		c.mutex.Lock()
		if c.syncedNum > 0 {
			c.syncedNum = c.syncedNum - 1
			if c.syncedNum == 0 {
				close(c.syncedCh)
			}
		}
		c.mutex.Unlock()
	}
}

func (c *Controller) processNextWorkItem() bool {
	obj, shutdown := c.workqueue.Get()
	if shutdown {
		return false
	}

	err := func(obj interface{}) error {
		defer c.workqueue.Done(obj)

		var key string
		var ok bool
		if key, ok = obj.(string); !ok {
			c.workqueue.Forget(obj)
			return fmt.Errorf("expected string in workqueue but got %#v", obj)
		}

		if err := c.eventHandler(key); err != nil {
			c.workqueue.AddRateLimited(key)
			return err
		}

		c.workqueue.Forget(obj)
		return nil
	}(obj)
	if err != nil {
		c.log.Error(err, "failed to process")
	}

	return true
}

// eventHandler is called when an event about cluster is received.
func (c *Controller) eventHandler(key string) error {
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		c.log.Error(err, "failed to split namespaced key", "key", key)
		return nil
	}

	obj, err := c.client.Resource(c.clusterProvider.GetClusterMangementGVR()).Namespace(namespace).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			c.mutex.Lock()
			defer c.mutex.Unlock()

			oldObj, ok := c.namespacedKeyToObj[key]
			if !ok {
				return nil
			}
			delete(c.namespacedKeyToObj, key)

			clusterName := c.clusterProvider.GetClusterName(oldObj)
			delete(c.clusterNameToNamespacedKey, clusterName)

			metrics.NewClusterEventCountMetrics(key, "delete", "true").Inc()
			c.deleteHandler(clusterName)
			return nil
		}
		metrics.NewClusterEventCountMetrics(key, "delete", "false").Inc()
		c.log.Error(err, "failed to get resource", "key", key)
		return err
	}

	c.mutex.Lock()
	c.namespacedKeyToObj[key] = obj
	clusterName := c.clusterProvider.GetClusterName(obj)
	c.clusterNameToNamespacedKey[clusterName] = key
	c.mutex.Unlock()

	err = c.addUpdateHandler(clusterName)
	if err != nil {
		metrics.NewClusterEventCountMetrics(key, "add-update", "false").Inc()
		c.log.Error(err, "failed to add or update cluster", "key", key)
		return err
	}

	metrics.NewClusterEventCountMetrics(key, "add-update", "true").Inc()
	return nil
}

// RestConfigForCluster returns the rest config for the mangered cluster.
func (c *Controller) RestConfigForCluster(clusterName string) *rest.Config {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	namespacedKey, ok := c.clusterNameToNamespacedKey[clusterName]
	if !ok {
		return nil
	}

	obj, ok := c.namespacedKeyToObj[namespacedKey]
	if !ok {
		return nil
	}
	return c.clusterProvider.GetClusterConfig(obj)
}
