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
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	toolscache "k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"kusionstack.io/kube-utils/multicluster/clusterinfo"
	"kusionstack.io/kube-utils/multicluster/metrics"
)

type ClusterCacheManager interface {
	AddClusterCache(cluster string, clusterCache cache.Cache)
	RemoveClusterCache(cluster string) bool
}

func MultiClusterCacheBuilder(log logr.Logger, managerOption *Options) (cache.NewCacheFunc, ClusterCacheManager) {
	mcc := &multiClusterCache{
		clusterToCache:   map[string]cache.Cache{},
		clusterToCancel:  map[string]context.CancelFunc{},
		objectToInformer: map[client.Object]*multiClusterInformer{},
		gvkToInformer:    map[schema.GroupVersionKind]*multiClusterInformer{},

		log: log,
	}

	newCacheFunc := func(config *rest.Config, opts cache.Options) (cache.Cache, error) {
		fedCache, err := managerOption.NewCache(config, opts)
		if err != nil {
			return nil, fmt.Errorf("failed to create fed cache: %v", err)
		}

		mcc.fedCache = fedCache
		return mcc, nil
	}

	return newCacheFunc, mcc
}

type multiClusterCache struct {
	fedCache       cache.Cache            // cache for fed cluster
	clusterToCache map[string]cache.Cache // cluster to cache

	clusterCtx       context.Context                                   // context for all cluster caches
	started          bool                                              // whether all known cluster caches already started
	clusterToCancel  map[string]context.CancelFunc                     // cancel function for each cluster cache
	objectToInformer map[client.Object]*multiClusterInformer           // object to informer
	gvkToInformer    map[schema.GroupVersionKind]*multiClusterInformer // gvk to informer

	mutex sync.RWMutex
	log   logr.Logger
}

var _ cache.Cache = &multiClusterCache{}

func (mcc *multiClusterCache) AddClusterCache(cluster string, clusterCache cache.Cache) {
	mcc.mutex.Lock()
	defer mcc.mutex.Unlock()

	mcc.clusterToCache[cluster] = clusterCache
	mcc.log.Info("add cluster cache", "cluster", cluster, "other cluster caches started", mcc.started)

	// Wait for all cluster caches to start together
	if !mcc.started {
		return
	}

	// When a new cluster cache is added, we need to start it and add it to the multiClusterInformer for each object and gvk
	clusterCtx, cancel := context.WithCancel(mcc.clusterCtx)
	mcc.clusterToCancel[cluster] = cancel

	go func() {
		err := clusterCache.Start(clusterCtx)
		if err != nil {
			mcc.log.Error(err, "failed to start cluster cache", "cluster", cluster)
		}
	}()

	for k, v := range mcc.objectToInformer {
		if !v.isMulti() {
			continue
		}
		informer, err := clusterCache.GetInformer(clusterCtx, k)
		if err != nil {
			mcc.log.Error(err, "failed to get cluster infomer", "cluster", cluster)
			continue
		}
		v.addClusterInformer(cluster, informer)
	}
	for k, v := range mcc.gvkToInformer {
		if !v.isMulti() {
			continue
		}
		informer, err := clusterCache.GetInformerForKind(clusterCtx, k)
		if err != nil {
			mcc.log.Error(err, "failed to get cluster infomer", "cluster", cluster)
			continue
		}
		v.addClusterInformer(cluster, informer)
	}
}

func (mcc *multiClusterCache) RemoveClusterCache(cluster string) bool {
	mcc.mutex.Lock()
	defer mcc.mutex.Unlock()

	_, ok := mcc.clusterToCache[cluster]
	if !ok {
		return false
	}

	cancel := mcc.clusterToCancel[cluster]
	cancel()
	delete(mcc.clusterToCancel, cluster)
	delete(mcc.clusterToCache, cluster)
	mcc.log.Info("remove cluster cache", "cluster", cluster)

	for _, v := range mcc.objectToInformer {
		if !v.isMulti() {
			continue
		}
		v.removeClusterInformer(cluster)
	}
	for _, v := range mcc.gvkToInformer {
		if !v.isMulti() {
			continue
		}
		v.removeClusterInformer(cluster)
	}

	return true
}

func (mcc *multiClusterCache) GetInformer(ctx context.Context, obj client.Object) (cache.Informer, error) {
	var clusters []string
	clusters, enableMultiple, err := mcc.getClusters(ctx)
	if err != nil {
		mcc.log.Error(err, "failed to get clusters", "kind", reflect.TypeOf(obj).String())
		return nil, err
	}

	mcc.mutex.Lock()
	defer mcc.mutex.Unlock()

	if informer, ok := mcc.objectToInformer[obj]; ok {
		mcc.log.Info("found informer for object")
		return informer, nil
	}

	informers := map[string]cache.Informer{}
	for _, cluster := range clusters {
		if cluster == clusterinfo.Fed {
			informer, err := mcc.fedCache.GetInformer(ctx, obj)
			if err != nil {
				mcc.log.Error(err, "failed to get fed informer")
				return nil, err
			}
			informers[clusterinfo.Fed] = informer
		} else {
			c, ok := mcc.clusterToCache[cluster]
			if !ok {
				return nil, fmt.Errorf("invalid cluster: %s", cluster)
			}
			informer, err := c.GetInformer(ctx, obj)
			if err != nil {
				mcc.log.Error(err, "failed to get cluster informer", "cluster", cluster)
				return nil, err
			}
			informers[cluster] = informer
		}
	}

	mci := &multiClusterInformer{
		enableMultiple:    enableMultiple,
		kind:              reflect.TypeOf(obj).String(),
		clusterToInformer: informers,
		log:               mcc.log,
	}
	mcc.objectToInformer[obj] = mci

	return mci, nil
}

func (mcc *multiClusterCache) GetInformerForKind(ctx context.Context, kind schema.GroupVersionKind) (cache.Informer, error) {
	var clusters []string
	clusters, enableMultiple, err := mcc.getClusters(ctx)
	if err != nil {
		mcc.log.Error(err, "failed to get clusters")
		return nil, err
	}

	mcc.mutex.Lock()
	defer mcc.mutex.Unlock()

	if informer, ok := mcc.gvkToInformer[kind]; ok {
		mcc.log.Info("found informer for kind", "kind", kind.String())
		return informer, nil
	}

	informers := map[string]cache.Informer{}
	for _, cluster := range clusters {
		if cluster == clusterinfo.Fed {
			informer, err := mcc.fedCache.GetInformerForKind(ctx, kind)
			if err != nil {
				mcc.log.Error(err, "failed to get fed informer")
				return nil, err
			}
			informers[clusterinfo.Fed] = informer
		} else {
			c, ok := mcc.clusterToCache[cluster]
			if !ok {
				return nil, fmt.Errorf("invalid cluster: %s", cluster)
			}
			informer, err := c.GetInformerForKind(ctx, kind)
			if err != nil {
				mcc.log.Error(err, "failed to get cluster informer", "cluster", cluster)
				return nil, err
			}
			informers[cluster] = informer
		}
	}

	mci := &multiClusterInformer{
		enableMultiple:    enableMultiple,
		kind:              kind.String(),
		clusterToInformer: informers,
		log:               mcc.log,
	}
	mcc.gvkToInformer[kind] = mci

	return mci, nil
}

func (mcc *multiClusterCache) Start(ctx context.Context) error {
	mcc.log.Info("start multicluster cache ...")

	go func() {
		mcc.log.Info("start fed cache")

		err := mcc.fedCache.Start(ctx)
		if err != nil {
			mcc.log.Error(err, "failed to start fed cache")
		}
	}()
	mcc.clusterCtx = clusterinfo.WithCluster(ctx, clusterinfo.Clusters)

	mcc.mutex.Lock()
	for cluster, clusterCache := range mcc.clusterToCache {
		mcc.log.Info("start cluster cache", "cluster", cluster)

		clusterCtx, cancel := context.WithCancel(mcc.clusterCtx)
		mcc.clusterToCancel[cluster] = cancel

		go func(clusterCtx context.Context, cluster string, clusterCache cache.Cache) {
			err := clusterCache.Start(clusterCtx)
			if err != nil {
				mcc.log.Error(err, "failed to start cluster cache", "cluster", cluster)
				return
			}
		}(clusterCtx, cluster, clusterCache)
	}
	mcc.started = true
	mcc.log.Info("start multicluster cache finished", "started", mcc.started)
	mcc.mutex.Unlock()

	<-ctx.Done()
	return nil
}

func (mcc *multiClusterCache) WaitForCacheSync(ctx context.Context) bool {
	var clusters []string
	clusters, _, err := mcc.getClusters(ctx)
	if len(clusters) == 0 {
		clusters = []string{clusterinfo.Fed}
	} else if err != nil {
		mcc.log.Error(err, "failed to get clusters")
		return false
	}
	mcc.log.Info("wait for cache sync", "clusters", clusters)

	mcc.mutex.RLock()
	clusterToCache := mcc.clusterToCache
	mcc.mutex.RUnlock()

	synced := true
	for _, cluster := range clusters {
		if cluster == clusterinfo.Fed {
			if s := mcc.fedCache.WaitForCacheSync(ctx); !s {
				mcc.log.Info("fed cache not synced")
				synced = s
				break
			}
		} else {
			c, ok := clusterToCache[cluster]
			if !ok {
				mcc.log.Info("invalid cluster", "cluster", cluster)
				continue
			}
			if s := c.WaitForCacheSync(ctx); !s {
				mcc.log.Info("cluster cache not synced", "cluster", cluster)
				synced = s
				break
			}
		}
	}
	mcc.log.Info("wait finished", "clusters", clusters, "synced", synced)

	return synced
}

func (mcc *multiClusterCache) IndexField(ctx context.Context, obj client.Object, field string, extractValue client.IndexerFunc) error {
	var clusters []string
	clusters, _, err := mcc.getClusters(ctx)
	if err != nil {
		mcc.log.Error(err, "failed to get clusters")
		return err
	}

	mcc.mutex.RLock()
	clusterToCache := mcc.clusterToCache
	mcc.mutex.RUnlock()

	for _, cluster := range clusters {
		if cluster == clusterinfo.Fed {
			err := mcc.fedCache.IndexField(ctx, obj, field, extractValue)
			if err != nil {
				mcc.log.Error(err, "failed to index field for fed cache")
				return err
			}
		} else {
			c, ok := clusterToCache[cluster]
			if !ok {
				return fmt.Errorf("invalid cluster: %s", cluster)
			}
			err := c.IndexField(ctx, obj, field, extractValue)
			if err != nil {
				mcc.log.Error(err, "failed to index field for cluster cache", "cluster", cluster)
				return err
			}
		}
	}
	return nil
}

func (mcc *multiClusterCache) Get(ctx context.Context, key types.NamespacedName, obj client.Object) (err error) {
	var cluster string
	defer func() {
		if err == nil {
			attachClusterToObjects(cluster, obj)
		}
		metrics.NewCacheCountMetrics(cluster, "Get", err).Inc()
	}()

	cluster, err = getCluster(ctx, obj.GetLabels())
	if err != nil {
		mcc.log.Error(err, "failed to get cluster")
		return err
	}

	if cluster == clusterinfo.Fed {
		err = mcc.fedCache.Get(ctx, key, obj)
		return err
	}

	mcc.mutex.RLock()
	clusterToCache := mcc.clusterToCache
	mcc.mutex.RUnlock()

	clusterCache, ok := clusterToCache[cluster]
	if !ok {
		return fmt.Errorf("unable to get: %v because of unknown cluster: %s for the cache", key, cluster)
	}
	return clusterCache.Get(ctx, key, obj)
}

func (mcc *multiClusterCache) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) (err error) {
	listOpts := client.ListOptions{}
	listOpts.ApplyOptions(opts)

	clusters, _, err := mcc.getClusters(ctx)
	if err != nil {
		mcc.log.Error(err, "failed to get clusters")
		return err
	}

	mcc.mutex.RLock()
	clusterToCache := mcc.clusterToCache
	mcc.mutex.RUnlock()

	allItems, err := meta.ExtractList(list)
	if err != nil {
		return err
	}
	limitSet := listOpts.Limit > 0
	for _, cluster := range clusters {
		var c cache.Cache
		if cluster == clusterinfo.Fed {
			c = mcc.fedCache
		} else {
			var ok bool
			c, ok = clusterToCache[cluster]
			if !ok {
				return fmt.Errorf("unable to list because of unknown cluster: %s for the cache", cluster)
			}
		}

		listObj := list.DeepCopyObject().(client.ObjectList)
		err = c.List(ctx, listObj, &listOpts)
		metrics.NewClientCountMetrics(cluster, "List", err).Inc()
		if err != nil {
			return err
		}

		items, err := meta.ExtractList(listObj)
		if err != nil {
			return err
		}

		// Attach cluster name to each item
		attachClusterToObjects(cluster, items...)

		allItems = append(allItems, items...)

		if limitSet {
			listOpts.Limit -= int64(len(items))
			if listOpts.Limit == 0 {
				break
			}
		}
	}
	return meta.SetList(list, allItems)
}

func (mcc *multiClusterCache) getClusters(ctx context.Context) (clusters []string, enableMultiple bool, err error) {
	clusters, ok := clusterinfo.GetClusters(ctx)
	if !ok {
		return nil, false, fmt.Errorf("invalid context")
	}

	if err := checkClusters(clusters); err != nil {
		return nil, false, err
	}

	clusters, enableMultiple = mcc.convertClusters(clusters)
	return
}

func (mcc *multiClusterCache) convertClusters(clusters []string) ([]string, bool) {
	mcc.mutex.RLock()
	defer mcc.mutex.RUnlock()

	enableMultiple := false
	if len(clusters) == 1 && clusters[0] == clusterinfo.All {
		clusters = []string{clusterinfo.Fed}
		for cluster := range mcc.clusterToCache {
			clusters = append(clusters, cluster)
		}
	} else if len(clusters) == 1 && clusters[0] == clusterinfo.Clusters {
		clusters = []string{}
		for cluster := range mcc.clusterToCache {
			clusters = append(clusters, cluster)
		}
		enableMultiple = true
	}
	return clusters, enableMultiple
}

type multiClusterInformer struct {
	enableMultiple    bool // whether this informer is for managed clusters, or only for fed cluster
	kind              string
	clusterToInformer map[string]cache.Informer

	handler      toolscache.ResourceEventHandler
	resyncPeriod time.Duration
	indexers     toolscache.Indexers

	log   logr.Logger
	mutex sync.RWMutex
}

var _ cache.Informer = &multiClusterInformer{}

func (mci *multiClusterInformer) AddEventHandler(handler toolscache.ResourceEventHandler) {
	mci.mutex.RLock()
	defer mci.mutex.RUnlock()

	for cluster, informer := range mci.clusterToInformer {
		mci.log.Info("add event handler", "enableMultiple", mci.enableMultiple, "cluster", cluster, "kind", mci.kind)

		w := &wrapResourceEventHandler{
			cluster: cluster,
			handler: handler,
			log:     mci.log,
		}

		informer.AddEventHandler(w)
	}
}

func (mci *multiClusterInformer) AddEventHandlerWithResyncPeriod(handler toolscache.ResourceEventHandler, resyncPeriod time.Duration) {
	mci.mutex.RLock()
	defer mci.mutex.RUnlock()

	for cluster, informer := range mci.clusterToInformer {
		mci.log.Info("add event handler", "enableMultiple", mci.enableMultiple, "cluster", cluster, "kind", mci.kind)

		w := &wrapResourceEventHandler{
			cluster: cluster,
			handler: handler,
			log:     mci.log,
		}

		informer.AddEventHandlerWithResyncPeriod(w, resyncPeriod)
	}
}

func (mci *multiClusterInformer) AddIndexers(indexers toolscache.Indexers) error {
	mci.mutex.RLock()
	defer mci.mutex.RUnlock()

	for _, informer := range mci.clusterToInformer {
		err := informer.AddIndexers(indexers)
		if err != nil {
			return err
		}
	}
	return nil
}

func (mci *multiClusterInformer) HasSynced() bool {
	mci.mutex.RLock()
	defer mci.mutex.RUnlock()

	for _, informer := range mci.clusterToInformer {
		if ok := informer.HasSynced(); !ok {
			return ok
		}
	}
	return true
}

func (mci *multiClusterInformer) addClusterInformer(cluster string, clusterInformer cache.Informer) {
	mci.mutex.Lock()
	defer mci.mutex.Unlock()

	if mci.handler != nil && mci.resyncPeriod != 0 {
		clusterInformer.AddEventHandlerWithResyncPeriod(mci.handler, mci.resyncPeriod)
	} else if mci.handler != nil {
		clusterInformer.AddEventHandler(mci.handler)
	}

	if mci.indexers != nil {
		clusterInformer.AddIndexers(mci.indexers)
	}

	mci.clusterToInformer[cluster] = clusterInformer
}

func (mci *multiClusterInformer) removeClusterInformer(cluster string) {
	mci.mutex.Lock()
	delete(mci.clusterToInformer, cluster)
	mci.mutex.Unlock()
}

func (mci *multiClusterInformer) isMulti() bool {
	return mci.enableMultiple
}
