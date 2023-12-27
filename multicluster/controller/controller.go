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
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"

	"kusionstack.io/kube-utils/multicluster/metrics"
)

type ClusterManagermentType string

const (
	OpenClusterManagement ClusterManagermentType = "OpenClusterManagement"
	TestCluterManagement  ClusterManagermentType = "TestCluterManagement"
)

type Controller struct {
	config                 *rest.Config
	clusterManagermentType ClusterManagermentType
	clusterManagermentGVR  schema.GroupVersionResource

	client          dynamic.Interface // client to get cluster info
	informerFactory dynamicinformer.DynamicSharedInformerFactory
	informer        cache.SharedIndexInformer
	workqueue       workqueue.RateLimitingInterface

	mutex     sync.Mutex
	syncedNum int // number of synced cluster
	syncedCh  chan struct{}

	addUpdateHandler func(string) error // when cluster is added or updated, this handler will be called
	deleteHandler    func(string)       // when cluster is deleted, this handler will be called
	log              logr.Logger

	// for test
	restConfigForCluster func(cluster string) *rest.Config
}

type ControllerConfig struct {
	Config                 *rest.Config // config for cluster managerment
	ClusterManagermentType ClusterManagermentType
	ClusterManagermentGVR  *schema.GroupVersionResource
	ResyncPeriod           time.Duration // resync period for cluster managerment
	Log                    logr.Logger

	// for test
	RestConfigForCluster func(cluster string) *rest.Config
}

// NewController creates a new Controller which will process events about cluster.
func NewController(cfg *ControllerConfig) (*Controller, error) {
	var clusterManagermentGVR schema.GroupVersionResource
	switch cfg.ClusterManagermentType {
	case OpenClusterManagement:
		if cfg.ClusterManagermentGVR == nil {
			return nil, fmt.Errorf("ClusterManagermentGVR must be set when use %s", cfg.ClusterManagermentType)
		}
		clusterManagermentGVR = *cfg.ClusterManagermentGVR
	case TestCluterManagement:
		if cfg.ClusterManagermentGVR == nil || cfg.RestConfigForCluster == nil {
			return nil, fmt.Errorf("ClusterManagermentGVR and RestConfigForCluster must be set when use %s", cfg.ClusterManagermentType)
		}
		clusterManagermentGVR = *cfg.ClusterManagermentGVR
	default:
		return nil, fmt.Errorf("not support cluster managerment type: %d", cfg.ClusterManagermentType)
	}

	client, err := dynamic.NewForConfig(cfg.Config)
	if err != nil {
		return nil, err
	}
	informerFactory := dynamicinformer.NewDynamicSharedInformerFactory(client, cfg.ResyncPeriod)
	informer := informerFactory.ForResource(clusterManagermentGVR).Informer()

	return &Controller{
		config:                 cfg.Config,
		client:                 client,
		informerFactory:        informerFactory,
		clusterManagermentType: cfg.ClusterManagermentType,
		clusterManagermentGVR:  clusterManagermentGVR,
		informer:               informer,
		workqueue:              workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), clusterManagermentGVR.Resource),
		syncedCh:               make(chan struct{}),
		log:                    cfg.Log,

		restConfigForCluster: cfg.RestConfigForCluster,
	}, nil
}

// AddEventHandler adds handler for events about cluster.
func (c *Controller) AddEventHandler(addUpdateHandler func(string) error, deleteHandler func(string)) {
	c.informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: c.enqueueClusterExtension,
		UpdateFunc: func(old, new interface{}) {
			c.enqueueClusterExtension(new)
		},
		DeleteFunc: c.enqueueClusterExtension,
	})

	c.addUpdateHandler = addUpdateHandler
	c.deleteHandler = deleteHandler
}

func (c *Controller) Run(threadiness int, stopCh <-chan struct{}) error {
	defer runtime.HandleCrash()
	defer c.workqueue.ShutDown()

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

func (c *Controller) enqueueClusterExtension(obj interface{}) {
	var key string
	var err error
	if key, err = cache.MetaNamespaceKeyFunc(obj); err != nil {
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
			metrics.NewControllerEventCountMetrics(key, "failed").Inc()
			return err
		}

		metrics.NewControllerEventCountMetrics(key, "ok").Inc()
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

	_, err = c.client.Resource(c.clusterManagermentGVR).Namespace(namespace).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			c.deleteHandler(name)
			return nil
		}
		c.log.Error(err, "failed to get resource", "key", key)
		return err
	}

	err = c.addUpdateHandler(name)
	if err != nil {
		c.log.Error(err, "failed to add or update cluster", "key", key)
		return err
	}

	return nil
}

// RestConfigForCluster returns the rest config for the mangered cluster.
func (c *Controller) RestConfigForCluster(cluster string) *rest.Config {
	switch c.clusterManagermentType {
	case OpenClusterManagement:
		clusterConfig := *c.config
		clusterConfig.Host = fmt.Sprintf("%s/apis/%s/%s/%s/%s/proxy", clusterConfig.Host, c.clusterManagermentGVR.Group, c.clusterManagermentGVR.Version, c.clusterManagermentGVR.Resource, cluster)
		return &clusterConfig
	case TestCluterManagement:
		return c.restConfigForCluster(cluster)
	default:
		return nil
	}
}
