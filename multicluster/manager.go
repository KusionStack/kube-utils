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

package multicluster

import (
	"context"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2/klogr"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	"sigs.k8s.io/controller-runtime/pkg/cluster"

	"kusionstack.io/kube-utils/multicluster/clusterinfo"
	"kusionstack.io/kube-utils/multicluster/controller"
)

const (
	EnvOnlyWatchClusterNamespace = "ONLY_WATCH_CLUSTER_NAMESPACE"
)

type Options struct {
	// NewCache is the function that will create the cache to be used
	// by the manager. If not set this will use the default new cache function.
	NewCache cache.NewCacheFunc
}

func setOptionsDefaults(opts Options) Options {
	if opts.NewCache == nil {
		opts.NewCache = cache.New
	}

	return opts
}

type ManagerConfig struct {
	FedConfig     *rest.Config
	ClusterScheme *runtime.Scheme
	ResyncPeriod  time.Duration
	ClusterFilter func(string) bool // select cluster
	Log           logr.Logger

	// for test
	ClusterManagermentGVR *schema.GroupVersionResource
	RestConfigForCluster  func(cluster string) *rest.Config
}

type Manager struct {
	newCache      cache.NewCacheFunc // function to create cache for cluster
	clusterScheme *runtime.Scheme    // scheme which is used to create cache for cluster

	clusterCacheManager  ClusterCacheManager
	clusterClientManager ClusterClientManager
	controller           *controller.Controller // controller for cluster managerment

	resyncPeriod              time.Duration
	hasCluster                map[string]struct{} // whether cluster has been added
	clusterFilter             func(string) bool
	onlyWatchClusterNamespace string
	mutex                     sync.Mutex
	log                       logr.Logger
}

func NewManager(cfg *ManagerConfig, opts Options) (manager *Manager, newCacheFunc cache.NewCacheFunc, newClientFunc cluster.NewClientFunc, err error) {
	var log logr.Logger
	if cfg.Log != nil {
		log = cfg.Log
	} else {
		log = klogr.New()
	}

	if cfg.ClusterScheme == nil {
		cfg.ClusterScheme = scheme.Scheme
	}

	clusterFilter := cfg.ClusterFilter
	allowList := strings.TrimSpace(os.Getenv(clusterinfo.EnvClusterAllowList))
	if clusterFilter == nil && allowList != "" {
		clusters := strings.Split(allowList, ",")
		log.Info("allow list", "clusters", clusters)

		hasCluster := map[string]struct{}{}
		for _, cluster := range clusters {
			if _, ok := hasCluster[cluster]; ok {
				continue
			}
			hasCluster[cluster] = struct{}{}
		}
		clusterFilter = func(cluster string) bool {
			_, ok := hasCluster[cluster]
			return ok
		}
	}

	var clusterManagermentType controller.ClusterManagermentType
	if cfg.ClusterManagermentGVR != nil {
		clusterManagermentType = controller.TestCluterManagement
	} else {
		clusterManagermentType = controller.OpenClusterManagement
	}

	controller, err := controller.NewController(&controller.ControllerConfig{
		Config:                 cfg.FedConfig,
		ResyncPeriod:           cfg.ResyncPeriod,
		ClusterManagermentType: clusterManagermentType,
		Log:                    log,

		ClusterManagermentGVR: cfg.ClusterManagermentGVR,
		RestConfigForCluster:  cfg.RestConfigForCluster,
	})
	if err != nil {
		return nil, nil, nil, err
	}

	opts = setOptionsDefaults(opts)
	newCacheFunc, clusterCacheManager := MultiClusterCacheBuilder(log, &opts)
	newClientFunc, clusterClientManager := MultiClusterClientBuilder(log)

	manager = &Manager{
		clusterScheme: cfg.ClusterScheme,
		newCache:      opts.NewCache,

		clusterCacheManager:  clusterCacheManager,
		clusterClientManager: clusterClientManager,
		controller:           controller,

		resyncPeriod:              cfg.ResyncPeriod,
		hasCluster:                make(map[string]struct{}),
		clusterFilter:             clusterFilter,
		onlyWatchClusterNamespace: strings.TrimSpace(os.Getenv(EnvOnlyWatchClusterNamespace)),
		log:                       log,
	}
	return manager, newCacheFunc, newClientFunc, nil
}

func (m *Manager) Run(threadiness int, ctx context.Context) error {
	stopCh := make(chan struct{})
	go func() {
		<-ctx.Done()
		close(stopCh)
	}()

	m.controller.AddEventHandler(m.addUpdateHandler, m.deleteHandler)
	return m.controller.Run(threadiness, stopCh)
}

func (m *Manager) WaitForSynced(ctx context.Context) bool {
	m.log.Info("wait for controller synced")
	return m.controller.WaitForSynced(ctx)
}

func (m *Manager) addUpdateHandler(cluster string) (err error) {
	if m.clusterFilter != nil && !m.clusterFilter(cluster) {
		return nil
	}

	m.mutex.Lock()
	if _, ok := m.hasCluster[cluster]; ok {
		m.log.V(5).Info("has cluster", "cluster", cluster)
		m.mutex.Unlock()
		return nil
	}
	m.mutex.Unlock()

	// Get rest.Config for the cluster
	cfg := m.controller.RestConfigForCluster(cluster)

	// Create cache for the cluster
	mapper, err := apiutil.NewDynamicRESTMapper(cfg)
	if err != nil {
		return err
	}
	clusterCache, err := m.newCache(cfg, cache.Options{
		Scheme:    m.clusterScheme,
		Mapper:    mapper,
		Resync:    &m.resyncPeriod,
		Namespace: m.onlyWatchClusterNamespace,
	})
	if err != nil {
		return err
	}

	// Create delegating client for the cluster
	clusterClient, err := client.New(cfg, client.Options{
		Scheme: m.clusterScheme,
		Mapper: mapper,
	})
	if err != nil {
		return err
	}
	delegatingClusterClient, err := client.NewDelegatingClient(client.NewDelegatingClientInput{
		CacheReader:       clusterCache,
		Client:            clusterClient,
		CacheUnstructured: true,
	})
	if err != nil {
		return err
	}

	m.log.Info("add cluster", "cluster", cluster)
	m.clusterCacheManager.AddClusterCache(cluster, clusterCache)
	m.clusterClientManager.AddClusterClient(cluster, delegatingClusterClient)

	m.mutex.Lock()
	m.hasCluster[cluster] = struct{}{}
	m.mutex.Unlock()

	return nil
}

func (m *Manager) deleteHandler(cluster string) {
	if m.clusterFilter != nil && !m.clusterFilter(cluster) {
		m.log.Info("ignore cluster", "cluster", cluster)
		return
	}

	m.log.Info("delete cluster", "cluster", cluster)
	m.clusterCacheManager.RemoveClusterCache(cluster)
	m.clusterClientManager.RemoveClusterClient(cluster)

	m.mutex.Lock()
	delete(m.hasCluster, cluster)
	m.mutex.Unlock()
}

func (m *Manager) SyncedClusters() []string {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	var clusters []string
	for cluster := range m.hasCluster {
		clusters = append(clusters, cluster)
	}
	return clusters
}
