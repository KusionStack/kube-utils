/*
Copyright 2020 The Kubernetes Authors.
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

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
	clienttoolcache "k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
)

// DefaultNewClient creates the default caching client.
func DefaultNewClient(cache cache.Cache, config *rest.Config, options client.Options, uncachedObjects ...client.Object) (client.Client, error) {
	c, err := client.New(config, options)
	if err != nil {
		return nil, err
	}

	return NewDelegatingClient(cache, client.NewDelegatingClientInput{
		Client:          c,
		UncachedObjects: uncachedObjects,
	})
}

// NewDelegatingClientInput encapsulates the input parameters to create a new delegating client.
type NewDelegatingClientInput struct {
	CacheReader       client.Reader
	Client            client.Client
	UncachedObjects   []client.Object
	CacheUnstructured bool
}

// NewDelegatingClient creates a new delegating client.
//
// A delegating client forms a Client by composing separate reader, writer and
// statusclient interfaces.  This way, you can have an Client that reads from a
// cache and writes to the API server.
func NewDelegatingClient(cache cache.Cache, in client.NewDelegatingClientInput) (client.Client, error) {
	uncachedGVKs := map[schema.GroupVersionKind]struct{}{}
	for _, obj := range in.UncachedObjects {
		gvk, err := apiutil.GVKForObject(obj, in.Client.Scheme())
		if err != nil {
			return nil, err
		}
		uncachedGVKs[gvk] = struct{}{}
	}

	return &delegatingClient{
		scheme: in.Client.Scheme(),
		mapper: in.Client.RESTMapper(),
		Reader: &delegatingReader{
			mapper:            in.Client.RESTMapper(),
			CacheReader:       cache,
			ClientReader:      in.Client,
			scheme:            in.Client.Scheme(),
			gvkScopeMapping:   map[schema.GroupVersionKind]meta.RESTScopeName{},
			uncachedGVKs:      uncachedGVKs,
			cacheUnstructured: in.CacheUnstructured,
		},
		Writer:       in.Client,
		StatusClient: in.Client,
	}, nil
}

var _ client.Client = &delegatingClient{}

type delegatingClient struct {
	client.Reader
	client.Writer
	client.StatusClient

	scheme *runtime.Scheme
	mapper meta.RESTMapper
}

// Scheme returns the scheme this client is using.
func (d *delegatingClient) Scheme() *runtime.Scheme {
	return d.scheme
}

// RESTMapper returns the rest mapper this client is using.
func (d *delegatingClient) RESTMapper() meta.RESTMapper {
	return d.mapper
}

// delegatingReader forms a Reader that will cause Get and List requests for
// unstructured types to use the ClientReader while requests for any other type
// of object with use the CacheReader.  This avoids accidentally caching the
// entire cluster in the common case of loading arbitrary unstructured objects
// (e.g. from OwnerReferences).
type delegatingReader struct {
	CacheReader  cache.Cache
	ClientReader client.Reader

	mapper            meta.RESTMapper
	gvkScopeMapping   map[schema.GroupVersionKind]meta.RESTScopeName
	uncachedGVKs      map[schema.GroupVersionKind]struct{}
	scheme            *runtime.Scheme
	cacheUnstructured bool
}

// Get retrieves an obj for a given object key from the Kubernetes Cluster.
func (d *delegatingReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object) error {
	var (
		realObj                 client.Object
		expectedDisableDeepCopy bool
	)
	if objectWithoutDeepCopy, ok := obj.(*ObjectWithoutDeepCopy); ok {
		expectedDisableDeepCopy = true
		realObj = objectWithoutDeepCopy.Object
	} else {
		realObj = obj
	}

	if isUncached, err := d.shouldBypassCache(realObj); err != nil {
		return err
	} else if isUncached {
		if expectedDisableDeepCopy {
			return fmt.Errorf("uncached GVK can not support DisableDeepCopy feature")
		}
		return d.ClientReader.Get(ctx, key, realObj)
	}

	if expectedDisableDeepCopy {
		return d.GetWithoutDeepCopy(ctx, key, realObj)
	} else {
		return d.CacheReader.Get(ctx, key, realObj)
	}
}

func (d *delegatingReader) shouldBypassCache(obj runtime.Object) (bool, error) {
	gvk, err := apiutil.GVKForObject(obj, d.scheme)
	if err != nil {
		return false, err
	}
	// TODO: this is producing unsafe guesses that don't actually work,
	// but it matches ~99% of the cases out there.
	if meta.IsListType(obj) {
		gvk.Kind = strings.TrimSuffix(gvk.Kind, "List")
	}
	if _, isUncached := d.uncachedGVKs[gvk]; isUncached {
		return true, nil
	}
	if !d.cacheUnstructured {
		_, isUnstructured := obj.(*unstructured.Unstructured)
		_, isUnstructuredList := obj.(*unstructured.UnstructuredList)
		return isUnstructured || isUnstructuredList, nil
	}
	return false, nil
}

func (d *delegatingReader) GetWithoutDeepCopy(ctx context.Context, key client.ObjectKey, out client.Object) error {
	gvk, err := apiutil.GVKForObject(out, d.scheme)
	if err != nil {
		return err
	}

	informer, err := d.CacheReader.GetInformerForKind(ctx, gvk)
	if err != nil {
		return err
	}

	sharedInformer, ok := informer.(clienttoolcache.SharedIndexInformer)
	if !ok {
		return fmt.Errorf("informer from cache is not of SharedIndexInformer")
	}

	scope, ok := d.gvkScopeMapping[gvk]
	if !ok {
		rm, err := d.mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
		if err != nil {
			return err
		}

		d.gvkScopeMapping[gvk] = rm.Scope.Name()
		scope = rm.Scope.Name()
	}

	if scope == meta.RESTScopeNameRoot {
		key.Namespace = ""
	}

	indexer := sharedInformer.GetIndexer()
	storeKey := objectKeyToStoreKey(key)
	// Lookup the object from the indexer cache
	obj, exists, err := indexer.GetByKey(storeKey)
	if err != nil {
		return err
	}

	// Not found, return an error
	if !exists {
		// Resource gets transformed into Kind in the error anyway, so this is fine
		return apierrors.NewNotFound(schema.GroupResource{
			Group:    gvk.Group,
			Resource: gvk.Kind,
		}, key.Name)
	}

	// Verify the result is a runtime.Object
	if _, isObj := obj.(runtime.Object); !isObj {
		// This should never happen
		return fmt.Errorf("cache contained %T, which is not an Object", obj)
	}

	// Copy the value of the item in the cache to the returned value
	outVal := reflect.ValueOf(out)
	objVal := reflect.ValueOf(obj)
	if !objVal.Type().AssignableTo(outVal.Type()) {
		return fmt.Errorf("cache had type %s, but %s was asked for", objVal.Type(), outVal.Type())
	}
	reflect.Indirect(outVal).Set(reflect.Indirect(objVal))
	out.GetObjectKind().SetGroupVersionKind(gvk)

	return nil
}

// List retrieves list of objects for a given namespace and list options.
func (d *delegatingReader) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if isUncached, err := d.shouldBypassCache(list); err != nil {
		return err
	} else if isUncached {
		return d.ClientReader.List(ctx, list, opts...)
	}
	return d.CacheReader.List(ctx, list, opts...)
}
