/*
Copyright 2018 The Kubernetes Authors.
Copyright 2023 The KusionStack Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cache

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/client-go/rest"
	clienttoolcache "k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
)

func NewCacheSupportDisableDeepCopy(config *rest.Config, opts cache.Options) (cache.Cache, error) {
	cache, err := cache.New(config, opts)
	if err != nil {
		return nil, err
	}

	return &cacheSupportDisableDeepCopy{
		Cache:  cache,
		scheme: opts.Scheme,
	}, nil
}

// cacheSupportDisableDeepCopy is a cache.Cache.
var _ cache.Cache = &cacheSupportDisableDeepCopy{}

type cacheSupportDisableDeepCopy struct {
	cache.Cache

	scheme *runtime.Scheme
}

func (c *cacheSupportDisableDeepCopy) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	for _, opt := range opts {
		if opt == DisableDeepCopy {
			return c.ListWithoutDeepCopy(ctx, list, opts...)
		}
	}

	return c.Cache.List(ctx, list, opts...)
}

func (c *cacheSupportDisableDeepCopy) ListWithoutDeepCopy(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	gvk, _, err := c.objectTypeForListObject(list)
	if err != nil {
		return err
	}

	informer, err := c.Cache.GetInformerForKind(ctx, *gvk)
	if err != nil {
		return err
	}

	sharedInformer, ok := informer.(clienttoolcache.SharedIndexInformer)
	if !ok {
		return fmt.Errorf("informer from cache is not of SharedIndexInformer")
	}

	listOpts := client.ListOptions{}
	listOpts.ApplyOptions(opts)

	if listOpts.Continue != "" {
		return fmt.Errorf("continue list option is not supported by the cache")
	}

	indexer := sharedInformer.GetIndexer()
	var objs []interface{}

	switch {
	case listOpts.FieldSelector != nil:
		requiresExact := RequiresExactMatch(listOpts.FieldSelector)
		if !requiresExact {
			return fmt.Errorf("non-exact field matches are not supported by the cache")
		}
		// list all objects by the field selector. If this is namespaced and we have one, ask for the
		// namespaced index key. Otherwise, ask for the non-namespaced variant by using the fake "all namespaces"
		// namespace.
		objs, err = byIndexes(indexer, listOpts.FieldSelector.Requirements(), listOpts.Namespace)
	case listOpts.Namespace != "":
		objs, err = indexer.ByIndex(clienttoolcache.NamespaceIndex, listOpts.Namespace)
	default:
		objs = indexer.List()
	}
	if err != nil {
		return err
	}
	var labelSel labels.Selector
	if listOpts.LabelSelector != nil {
		labelSel = listOpts.LabelSelector
	}

	limitSet := listOpts.Limit > 0

	runtimeObjs := make([]runtime.Object, 0, len(objs))
	for _, item := range objs {
		// if the Limit option is set and the number of items
		// listed exceeds this limit, then stop reading.
		if limitSet && int64(len(runtimeObjs)) >= listOpts.Limit {
			break
		}
		obj, isObj := item.(runtime.Object)
		if !isObj {
			return fmt.Errorf("cache contained %T, which is not an Object", item)
		}
		meta, err := apimeta.Accessor(obj)
		if err != nil {
			return err
		}
		if labelSel != nil {
			lbls := labels.Set(meta.GetLabels())
			if !labelSel.Matches(lbls) {
				continue
			}
		}

		runtimeObjs = append(runtimeObjs, obj)
	}
	return apimeta.SetList(list, runtimeObjs)
}

// objectTypeForListObject tries to find the runtime.Object and associated GVK
// for a single object corresponding to the passed-in list type. We need them
// because they are used as cache map key.
func (ip *cacheSupportDisableDeepCopy) objectTypeForListObject(list client.ObjectList) (*schema.GroupVersionKind, runtime.Object, error) {
	gvk, err := apiutil.GVKForObject(list, ip.scheme)
	if err != nil {
		return nil, nil, err
	}

	if !strings.HasSuffix(gvk.Kind, "List") {
		return nil, nil, fmt.Errorf("non-list type %T (kind %q) passed as output", list, gvk)
	}
	// we need the non-list GVK, so chop off the "List" from the end of the kind
	gvk.Kind = gvk.Kind[:len(gvk.Kind)-4]
	_, isUnstructured := list.(*unstructured.UnstructuredList)
	var cacheTypeObj runtime.Object
	if isUnstructured {
		u := &unstructured.Unstructured{}
		u.SetGroupVersionKind(gvk)
		cacheTypeObj = u
	} else {
		itemsPtr, err := apimeta.GetItemsPtr(list)
		if err != nil {
			return nil, nil, err
		}
		// http://knowyourmeme.com/memes/this-is-fine
		elemType := reflect.Indirect(reflect.ValueOf(itemsPtr)).Type().Elem()
		if elemType.Kind() != reflect.Ptr {
			elemType = reflect.PointerTo(elemType)
		}

		cacheTypeValue := reflect.Zero(elemType)
		var ok bool
		cacheTypeObj, ok = cacheTypeValue.Interface().(runtime.Object)
		if !ok {
			return nil, nil, fmt.Errorf("cannot get cache for %T, its element %T is not a runtime.Object", list, cacheTypeValue.Interface())
		}
	}

	return &gvk, cacheTypeObj, nil
}

// RequiresExactMatch checks if the given field selector is of the form `k=v` or `k==v`.
func RequiresExactMatch(sel fields.Selector) bool {
	reqs := sel.Requirements()
	if len(reqs) == 0 {
		return false
	}

	for _, req := range reqs {
		if req.Operator != selection.Equals && req.Operator != selection.DoubleEquals {
			return false
		}
	}
	return true
}

func byIndexes(indexer clienttoolcache.Indexer, requires fields.Requirements, namespace string) ([]interface{}, error) {
	var (
		err  error
		objs []interface{}
		vals []string
	)
	indexers := indexer.GetIndexers()
	for idx, req := range requires {
		indexName := FieldIndexName(req.Field)
		indexedValue := KeyToNamespacedKey(namespace, req.Value)
		if idx == 0 {
			// we use first require to get snapshot data
			// TODO(halfcrazy): use complicated index when client-go provides byIndexes
			// https://github.com/kubernetes/kubernetes/issues/109329
			objs, err = indexer.ByIndex(indexName, indexedValue)
			if err != nil {
				return nil, err
			}
			if len(objs) == 0 {
				return nil, nil
			}
			continue
		}
		fn, exist := indexers[indexName]
		if !exist {
			return nil, fmt.Errorf("index with name %s does not exist", indexName)
		}
		filteredObjects := make([]interface{}, 0, len(objs))
		for _, obj := range objs {
			vals, err = fn(obj)
			if err != nil {
				return nil, err
			}
			for _, val := range vals {
				if val == indexedValue {
					filteredObjects = append(filteredObjects, obj)
					break
				}
			}
		}
		if len(filteredObjects) == 0 {
			return nil, nil
		}
		objs = filteredObjects
	}
	return objs, nil
}

// FieldIndexName constructs the name of the index over the given field,
// for use with an indexer.
func FieldIndexName(field string) string {
	return "field:" + field
}

// allNamespacesNamespace is used as the "namespace" when we want to list across all namespaces.
const allNamespacesNamespace = "__all_namespaces"

// KeyToNamespacedKey prefixes the given index key with a namespace
// for use in field selector indexes.
func KeyToNamespacedKey(ns string, baseKey string) string {
	if ns != "" {
		return ns + "/" + baseKey
	}
	return allNamespacesNamespace + "/" + baseKey
}

// objectKeyToStorageKey converts an object key to store key.
// It's akin to MetaNamespaceKeyFunc.  It's separate from
// String to allow keeping the key format easily in sync with
// MetaNamespaceKeyFunc.
func objectKeyToStoreKey(k client.ObjectKey) string {
	if k.Namespace == "" {
		return k.Name
	}
	return k.Namespace + "/" + k.Name
}

type ObjectWithoutDeepCopy struct {
	client.Object
}

var DisableDeepCopy = &disableDeepCopyListOption{}

type disableDeepCopyListOption struct{}

func (o *disableDeepCopyListOption) ApplyToList(*client.ListOptions) {
}
