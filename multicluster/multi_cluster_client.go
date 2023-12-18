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
	"sync"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/cluster"

	"kusionstack.io/kube-utils/multicluster/clusterinfo"
	"kusionstack.io/kube-utils/multicluster/metrics"
)

type ClusterClientManager interface {
	AddClusterClient(cluster string, clusterClient client.Client)
	RemoveClusterClient(cluster string) bool
}

func MultiClusterClientBuilder(log logr.Logger) (cluster.NewClientFunc, ClusterClientManager) {
	mcc := &multiClusterClient{
		clusterToClient: map[string]client.Client{},
		log:             log,
	}

	newClientFunc := func(cache cache.Cache, config *rest.Config, options client.Options, uncachedObjects ...client.Object) (client.Client, error) {
		fedClient, err := client.New(config, options)
		if err != nil {
			return nil, fmt.Errorf("failed to create fed client: %v", err)
		}

		delegatingFedClient, err := client.NewDelegatingClient(client.NewDelegatingClientInput{
			CacheReader:     cache,
			Client:          fedClient,
			UncachedObjects: uncachedObjects,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create fed client: %v", err)
		}

		mcc.fedClient = delegatingFedClient
		mcc.fedScheme = delegatingFedClient.Scheme()
		mcc.fedMapper = delegatingFedClient.RESTMapper()
		return mcc, nil
	}

	return newClientFunc, mcc
}

type multiClusterClient struct {
	fedClient client.Client
	fedScheme *runtime.Scheme
	fedMapper meta.RESTMapper

	clusterToClient map[string]client.Client
	mutex           sync.RWMutex
	log             logr.Logger
}

var _ client.Client = &multiClusterClient{}

func (mcc *multiClusterClient) AddClusterClient(cluster string, clusterClient client.Client) {
	mcc.mutex.Lock()
	mcc.clusterToClient[cluster] = clusterClient
	mcc.log.V(5).Info("add cluster client", "cluster", cluster)
	mcc.mutex.Unlock()
}

func (mcc *multiClusterClient) RemoveClusterClient(cluster string) bool {
	mcc.mutex.Lock()
	defer mcc.mutex.Unlock()

	_, ok := mcc.clusterToClient[cluster]
	if !ok {
		return false
	}

	delete(mcc.clusterToClient, cluster)
	mcc.log.V(5).Info("remove cluster client", "cluster", cluster)
	return true
}

func (mcc *multiClusterClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) (err error) {
	var cluster string
	defer func() {
		metrics.NewClientCountMetrics(cluster, "Create", err)
	}()

	cluster, err = getThenDeleteClusterName(ctx, obj.GetLabels())
	if err != nil {
		mcc.log.Error(err, "failed to get cluster")
		return err
	}

	if cluster == clusterinfo.Fed {
		return mcc.fedClient.Create(ctx, obj, opts...)
	}

	mcc.mutex.RLock()
	defer mcc.mutex.RUnlock()

	clusterClient, ok := mcc.clusterToClient[cluster]
	if !ok {
		return fmt.Errorf("unable to create: %v because of unknown cluster: %s for the client", obj, cluster)
	}
	return clusterClient.Create(ctx, obj, opts...)
}

func (mcc *multiClusterClient) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) (err error) {
	var cluster string
	defer func() {
		metrics.NewClientCountMetrics(cluster, "Delete", err).Inc()
	}()

	cluster, err = getClusterName(ctx, obj.GetLabels())
	if err != nil {
		mcc.log.Error(err, "failed to get cluster")
		return err
	}

	if cluster == clusterinfo.Fed {
		return mcc.fedClient.Delete(ctx, obj, opts...)
	}

	mcc.mutex.RLock()
	defer mcc.mutex.RUnlock()

	clusterClient, ok := mcc.clusterToClient[cluster]
	if !ok {
		return fmt.Errorf("unable to delete: %v because of unknown cluster: %s for the client", obj, cluster)
	}
	return clusterClient.Delete(ctx, obj, opts...)
}

func (mcc *multiClusterClient) DeleteAllOf(ctx context.Context, obj client.Object, opts ...client.DeleteAllOfOption) (err error) {
	var cluster string
	defer func() {
		metrics.NewClientCountMetrics(cluster, "DeleteAllOf", err).Inc()
	}()

	cluster, err = getClusterName(ctx, obj.GetLabels())
	if err != nil {
		mcc.log.Error(err, "failed to get cluster")
		return err
	}

	if cluster == clusterinfo.Fed {
		return mcc.fedClient.DeleteAllOf(ctx, obj, opts...)
	}

	mcc.mutex.RLock()
	defer mcc.mutex.RUnlock()

	clusterClient, ok := mcc.clusterToClient[cluster]
	if !ok {
		err = fmt.Errorf("unable to deleteAllOf: %v because of unknown cluster: %s for the client", obj, cluster)
		return
	}
	err = clusterClient.DeleteAllOf(ctx, obj, opts...)
	return
}

func (mcc *multiClusterClient) Get(ctx context.Context, key types.NamespacedName, obj client.Object) (err error) {
	var cluster string
	defer func() {
		if err == nil {
			attachClusterTo(obj, cluster)
		}
		metrics.NewClientCountMetrics(cluster, "Get", err).Inc()
	}()

	cluster, err = getClusterName(ctx, obj.GetLabels())
	if err != nil {
		mcc.log.Error(err, "failed to get cluster")
		return err
	}

	if cluster == clusterinfo.Fed {
		return mcc.fedClient.Get(ctx, key, obj)
	}

	mcc.mutex.RLock()
	defer mcc.mutex.RUnlock()

	clusterClient, ok := mcc.clusterToClient[cluster]
	if !ok {
		return fmt.Errorf("unable to get: %v because of unknown cluster: %s for the client", obj, cluster)
	}
	return clusterClient.Get(ctx, key, obj)
}

func (mcc *multiClusterClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) (err error) {
	listOpts := client.ListOptions{}
	listOpts.ApplyOptions(opts)

	clusters, err := mcc.getClusterNames(ctx)
	if err != nil {
		mcc.log.Error(err, "failed to get clusters")
		return err
	}

	allItems, err := meta.ExtractList(list)
	if err != nil {
		return err
	}

	mcc.mutex.RLock()
	defer mcc.mutex.RUnlock()

	limitSet := listOpts.Limit > 0

	for _, cluster := range clusters {
		var c client.Client
		if cluster == clusterinfo.Fed {
			c = mcc.fedClient
		} else {
			var ok bool
			c, ok = mcc.clusterToClient[cluster]
			if !ok {
				return fmt.Errorf("unable to list because of unknown cluster: %s for the client", cluster)
			}
		}

		listObj := list.DeepCopyObject().(client.ObjectList)
		err = c.List(ctx, listObj, &listOpts)
		metrics.NewClientCountMetrics(cluster, "List", err).Inc()
		if err != nil {
			return err
		}

		attachClusterTo(listObj, cluster)
		items, err := meta.ExtractList(listObj)
		if err != nil {
			return err
		}

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

func (mcc *multiClusterClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) (err error) {
	var cluster string
	defer func() {
		if err == nil {
			attachClusterTo(obj, cluster)
		}
		metrics.NewClientCountMetrics(cluster, "Patch", err).Inc()
	}()

	cluster, err = getThenDeleteClusterName(ctx, obj.GetLabels())
	if err != nil {
		mcc.log.Error(err, "failed to get cluster")
		return err
	}

	if cluster == clusterinfo.Fed {
		return mcc.fedClient.Patch(ctx, obj, patch, opts...)
	}

	mcc.mutex.RLock()
	defer mcc.mutex.RUnlock()

	clusterClient, ok := mcc.clusterToClient[cluster]
	if !ok {
		return fmt.Errorf("unable to patch: %v because of unknown cluster: %v for the client", obj, cluster)
	}
	return clusterClient.Patch(ctx, obj, patch, opts...)
}

func (mcc *multiClusterClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) (err error) {
	var cluster string
	defer func() {
		if err == nil {
			attachClusterTo(obj, cluster)
		}
		metrics.NewClientCountMetrics(cluster, "Update", err).Inc()
	}()

	cluster, err = getThenDeleteClusterName(ctx, obj.GetLabels())
	if err != nil {
		mcc.log.Error(err, "failed to get cluster")
		return err
	}

	if cluster == clusterinfo.Fed {
		return mcc.fedClient.Update(ctx, obj, opts...)
	}

	mcc.mutex.RLock()
	defer mcc.mutex.RUnlock()

	clusterClient, ok := mcc.clusterToClient[cluster]
	if !ok {
		err = fmt.Errorf("unable to update: %v because of unknown cluster: %s for the client", obj, cluster)
		return
	}
	return clusterClient.Update(ctx, obj, opts...)
}

func (mcc *multiClusterClient) RESTMapper() meta.RESTMapper {
	return mcc.fedMapper
}

func (mcc *multiClusterClient) Scheme() *runtime.Scheme {
	return mcc.fedScheme
}

func (mcc *multiClusterClient) Status() client.StatusWriter {
	return &statusWriter{
		fedClient:       mcc.fedClient,
		clusterToClient: mcc.clusterToClient,
		log:             mcc.log,
	}
}

type statusWriter struct {
	fedClient       client.Client
	clusterToClient map[string]client.Client
	log             logr.Logger
}

func (sw *statusWriter) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) (err error) {
	var cluster string
	defer func() {
		if err == nil {
			attachClusterTo(obj, cluster)
		}
		metrics.NewClientCountMetrics(cluster, "StatusUpdate", err).Inc()
	}()

	// Should not write cluster info into apiserver
	cluster, err = getThenDeleteClusterName(ctx, obj.GetLabels())
	if err != nil {
		sw.log.Error(err, "failed to get cluster")
		return err
	}

	if cluster == clusterinfo.Fed {
		return sw.fedClient.Status().Update(ctx, obj, opts...)
	}

	clusterClient, ok := sw.clusterToClient[cluster]
	if !ok {
		return fmt.Errorf("unable to update: %v because of unknown cluster: %s for the client", obj, cluster)
	}
	return clusterClient.Status().Update(ctx, obj, opts...)
}

func (sw *statusWriter) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) (err error) {
	var cluster string
	defer func() {
		if err == nil {
			attachClusterTo(obj, cluster)
		}
		metrics.NewClientCountMetrics(cluster, "StatusPatch", err).Inc()
	}()

	// Should not write cluster info into apiserver
	cluster, err = getThenDeleteClusterName(ctx, obj.GetLabels())
	if err != nil {
		sw.log.Error(err, "failed to get cluster")
		return err
	}

	if cluster == clusterinfo.Fed {
		return sw.fedClient.Status().Patch(ctx, obj, patch, opts...)
	}

	clusterClient, ok := sw.clusterToClient[cluster]
	if !ok {
		return fmt.Errorf("unable to update: %v because of unknown cluster: %s for the client", obj, cluster)
	}
	return clusterClient.Status().Patch(ctx, obj, patch, opts...)
}

func (mcc *multiClusterClient) getClusterNames(ctx context.Context) (clusters []string, err error) {
	mcc.mutex.RLock()
	defer mcc.mutex.RUnlock()

	clusters, ok := clusterinfo.GetClusters(ctx)
	if !ok {
		return nil, fmt.Errorf("invalid context")
	}

	if err := checkClusters(clusters); err != nil {
		return nil, err
	}

	if len(clusters) == 1 && clusters[0] == clusterinfo.All {
		clusters = []string{clusterinfo.Fed}
		for cluster := range mcc.clusterToClient {
			clusters = append(clusters, cluster)
		}
	} else if len(clusters) == 1 && clusters[0] == clusterinfo.Clusters {
		clusters = []string{}
		for cluster := range mcc.clusterToClient {
			clusters = append(clusters, cluster)
		}
	}
	return
}
