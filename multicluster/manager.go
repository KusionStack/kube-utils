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
	"errors"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2/klogr"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	"sigs.k8s.io/controller-runtime/pkg/cluster"

	"kusionstack.io/kube-utils/multicluster/clusterinfo"
)

const (
	EnvOnlyWatchClusterNamespace = "ONLY_WATCH_CLUSTER_NAMESPACE"
)

type Options struct {
	// NewCache is the function to create the cache to be used by the manager. Default is cache.New.
	NewCache cache.NewCacheFunc
}

func setOptionsDefaults(opts Options) Options {
	if opts.NewCache == nil {
		opts.NewCache = cache.New
	}

	return opts
}

type ClusterProvider interface {
	Run(stopCh <-chan struct{}) error                                                              // Run the ClusterProvider
	AddEventHandler(addUpdateHandler func(string, *rest.Config) error, deleteHandler func(string)) // Add event handler for cluster events
	WaitForSynced(ctx context.Context) bool                                                        // Wait for all clusters to be synced
}

type ManagerConfig struct {
	FedConfig       *rest.Config
	ClusterScheme   *runtime.Scheme
	ClusterProvider ClusterProvider

	ResyncPeriod  time.Duration
	ClusterFilter func(string) bool // select cluster
	Log           logr.Logger
}

type Manager struct {
	newCache      cache.NewCacheFunc // Function to create cache for cluster
	clusterScheme *runtime.Scheme    // Scheme which is used to create cache for cluster

	clusterProvider ClusterProvider

	clusterCacheManager  ClusterCacheManager
	clusterClientManager ClusterClientManager

	resyncPeriod              time.Duration
	hasCluster                map[string]struct{} // Whether cluster has been added
	clusterFilter             func(string) bool
	onlyWatchClusterNamespace string
	mutex                     sync.RWMutex
	log                       logr.Logger
}

func NewManager(cfg *ManagerConfig, opts Options) (manager *Manager, newCacheFunc cache.NewCacheFunc, newClientFunc cluster.NewClientFunc, err error) {
	if cfg.ClusterProvider == nil {
		return nil, nil, nil, errors.New("ClusterProvider is required")
	}

	var log logr.Logger
	if cfg.Log != nil {
		log = cfg.Log
	} else {
		log = klogr.New()
	}

	if cfg.ClusterScheme == nil {
		cfg.ClusterScheme = scheme.Scheme
	}

	opts = setOptionsDefaults(opts)
	newCacheFunc, clusterCacheManager := MultiClusterCacheBuilder(log, &opts)
	newClientFunc, clusterClientManager := MultiClusterClientBuilder(log)

	clusterFilter, err := getClusterFilter(cfg)
	if err != nil {
		return nil, nil, nil, err
	}

	manager = &Manager{
		clusterScheme: cfg.ClusterScheme,
		newCache:      opts.NewCache,

		clusterCacheManager:  clusterCacheManager,
		clusterClientManager: clusterClientManager,
		clusterProvider:      cfg.ClusterProvider,

		resyncPeriod:              cfg.ResyncPeriod,
		hasCluster:                make(map[string]struct{}),
		clusterFilter:             clusterFilter,
		onlyWatchClusterNamespace: strings.TrimSpace(os.Getenv(EnvOnlyWatchClusterNamespace)),
		log:                       log,
	}
	return manager, newCacheFunc, newClientFunc, nil
}

func (m *Manager) Run(ctx context.Context) error {
	stopCh := make(chan struct{})
	go func() {
		<-ctx.Done()
		close(stopCh)
	}()

	m.clusterProvider.AddEventHandler(m.addUpdateHandler, m.deleteHandler)
	return m.clusterProvider.Run(stopCh)
}

func (m *Manager) WaitForSynced(ctx context.Context) bool {
	m.log.Info("wait for ClusterProvider synced")
	return m.clusterProvider.WaitForSynced(ctx)
}

func (m *Manager) addUpdateHandler(cluster string, cfg *rest.Config) (err error) {
	if m.clusterFilter != nil && !m.clusterFilter(cluster) {
		return nil
	}

	m.mutex.RLock()
	if _, ok := m.hasCluster[cluster]; ok {
		m.log.V(5).Info("has cluster", "cluster", cluster)
		m.mutex.RUnlock()
		return nil
	}
	m.mutex.RUnlock()

	// Get MemCacheClient for the cluster
	discoveryClient, err := discovery.NewDiscoveryClientForConfig(cfg)
	if err != nil {
		return err
	}

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
	m.clusterClientManager.AddClusterClient(cluster, delegatingClusterClient, discoveryClient)

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

// getClusterFilter returns a function to filter clusters
func getClusterFilter(cfg *ManagerConfig) (func(string) bool, error) {
	var (
		log                = klogr.New()
		allowSet, blockSet sets.String
	)
	allowList := strings.TrimSpace(os.Getenv(clusterinfo.EnvClusterAllowList))
	if allowList != "" {
		allowSet = sets.NewString(strings.Split(allowList, ",")...)
	}

	blockList := strings.TrimSpace(os.Getenv(clusterinfo.EnvClusterBlockList))
	if blockList != "" {
		blockSet = sets.NewString(strings.Split(blockList, ",")...)
	}

	if allowSet != nil && blockSet != nil {
		return nil, errors.New("both cluster allow and block lists are set")
	}
	if cfg.ClusterFilter != nil {
		if allowSet != nil || blockSet != nil {
			return nil, errors.New("both cluster allow and block lists are set")
		}
		return cfg.ClusterFilter, nil
	}

	clusterFilter := func(cluster string) bool {
		if allowSet != nil && allowSet.Len() > 0 {
			has := allowSet.Has(cluster)
			log.V(4).Info("check cluster allow list", "cluster", cluster, "has", has)
			return has
		}
		if blockSet != nil && blockSet.Len() > 0 {
			has := blockSet.Has(cluster)
			log.V(4).Info("check cluster block list", "cluster", cluster, "has", has)
			return !has
		}
		return true // Default is allow all clusters
	}

	return clusterFilter, nil
}
