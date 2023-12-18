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

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/runtime"
	toolscache "k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/source"

	"kusionstack.io/kube-utils/multicluster/clusterinfo"
)

func FedKind(k *source.Kind) *KindWithClusters {
	return &KindWithClusters{
		Clusters: []string{clusterinfo.Fed},
		Kind:     k,
	}
}

func ClustersKind(k *source.Kind) *KindWithClusters {
	return &KindWithClusters{
		Clusters: []string{clusterinfo.Clusters},
		Kind:     k,
	}
}

type KindWithClusters struct {
	Clusters []string
	Kind     *source.Kind
}

var _ source.Source = &KindWithClusters{}
var _ source.SyncingSource = &KindWithClusters{}

func (k *KindWithClusters) InjectCache(c cache.Cache) error {
	return k.Kind.InjectCache(c)
}

func (k *KindWithClusters) Start(ctx context.Context, handler handler.EventHandler, queue workqueue.RateLimitingInterface, prct ...predicate.Predicate) error {
	return k.Kind.Start(clusterinfo.WithClusters(ctx, k.Clusters), handler, queue, prct...)
}

func (k *KindWithClusters) String() string {
	return k.Kind.String()
}

func (k *KindWithClusters) WaitForSync(ctx context.Context) error {
	return k.Kind.WaitForSync(ctx)
}

type wrapResourceEventHandler struct {
	cluster string
	handler toolscache.ResourceEventHandler
	log     logr.Logger
}

var _ toolscache.ResourceEventHandler = &wrapResourceEventHandler{}

type DeepCopy interface {
	DeepCopyObject() runtime.Object
}

func (w *wrapResourceEventHandler) OnAdd(obj interface{}) {
	copiedObj, attachErr := w.attachClusterTo("OnAdd", obj)
	if copiedObj == nil || attachErr != nil {
		w.log.V(3).Info("OnAdd", "cluster", w.cluster)
		w.handler.OnAdd(obj)
		return
	}

	w.handler.OnAdd(copiedObj)
}

func (w *wrapResourceEventHandler) OnUpdate(oldObj, newObj interface{}) {
	copiedOlbObj, attchOldErr := w.attachClusterTo("OnUpdate", oldObj)
	copiedNewObj, attachNewErr := w.attachClusterTo("OnUpdate", newObj)

	if copiedOlbObj == nil || attchOldErr != nil ||
		copiedNewObj == nil || attachNewErr != nil {
		w.log.V(3).Info("OnUpdate", "cluster", w.cluster)
		w.handler.OnUpdate(oldObj, newObj)
		return
	}

	w.handler.OnUpdate(copiedOlbObj, copiedNewObj)
}

func (w *wrapResourceEventHandler) OnDelete(obj interface{}) {
	copiedObj, attachErr := w.attachClusterTo("OnDelete", obj)
	if copiedObj == nil || attachErr != nil {
		w.log.V(3).Info("OnDelete", "cluster", w.cluster)
		w.handler.OnDelete(obj)
		return
	}

	w.handler.OnDelete(copiedObj)
}

func (w *wrapResourceEventHandler) attachClusterTo(handler string, obj interface{}) (copiedObj interface{}, attachErr error) {
	if o, ok := obj.(client.Object); ok {
		w.log.V(3).Info("attach cluster info into object", "handler", handler, "cluster", w.cluster, "namespace", o.GetNamespace(), "name", o.GetName(), "resource version", o.GetResourceVersion())

		copiedObj = o.DeepCopyObject()
		attachErr = attachClusterTo(copiedObj, w.cluster)
		return copiedObj, attachErr
	}
	if o, ok := obj.(DeepCopy); ok {
		w.log.V(3).Info("attach cluster info into object", "handler", handler, "cluster", w.cluster)

		copiedObj = o.DeepCopyObject()
		attachErr = attachClusterTo(copiedObj, w.cluster)
		return copiedObj, attachErr
	}
	return obj, fmt.Errorf("failed to attach cluster info into obj")
}
