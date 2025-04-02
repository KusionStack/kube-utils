// Copyright 2025 The KusionStack Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package expectations

import (
	"context"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/clock"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// CacheExpectationsInterface is an interface that allows users to set and wait on expectations.
// Only abstracted out for testing.
type CacheExpectationsInterface interface {
	GetExpectations(controllerKey string) (*CacheExpectation, bool, error)
	DeleteExpectations(controllerKey string)
	SatisfiedExpectations(controllerKey string) bool
	ExpectCreation(controllerKey string, gvk schema.GroupVersionKind, namespace, name string) error
	ExpectDeletion(controllerKey string, gvk schema.GroupVersionKind, namespace, name string) error
	ExpectUpdation(controllerKey string, gvk schema.GroupVersionKind, namespace, name, resourceVersion string) error
}

var _ CacheExpectationsInterface = &CacheExpectations{}

type CacheExpectations struct {
	cache.Store

	reader client.Reader
	scheme *runtime.Scheme
	clock  clock.Clock
}

func NewxCacheExpectations(reader client.Reader, scheme *runtime.Scheme, clock clock.Clock) *CacheExpectations {
	return &CacheExpectations{
		Store:  cache.NewStore(ExpKeyFunc),
		reader: reader,
		scheme: scheme,
		clock:  clock,
	}
}

// GetExpectations returns the ControlleeExpectations of the given controller.
func (r *CacheExpectations) GetExpectations(controllerKey string) (*CacheExpectation, bool, error) {
	exp, exists, err := r.GetByKey(controllerKey)
	if err == nil && exists {
		return exp.(*CacheExpectation), true, nil
	}
	return nil, false, err
}

// DeleteExpectations deletes the expectations of the given controller from the TTLStore.
func (r *CacheExpectations) DeleteExpectations(controllerKey string) {
	if exp, exists, err := r.GetByKey(controllerKey); err == nil && exists {
		if err := r.Delete(exp); err != nil {
			klog.V(2).Infof("Error deleting expectations for controller %v: %v", controllerKey, err)
		}
	}
}

func (r *CacheExpectations) SatisfiedExpectations(controllerKey string) bool {
	if exp, exists, err := r.GetExpectations(controllerKey); exists {
		if exp.Fulfilled() {
			klog.V(4).Infof("Cache expectations fulfilled %s", controllerKey)
			return true
		} else {
			klog.V(4).Infof("Controller still waiting on cache expectations %s", controllerKey)
			return false
		}
	} else if err != nil {
		klog.V(2).Infof("Error encountered while checking cache expectations %#v, forcing sync", err)
	} else {
		// When a new controller is created, it doesn't have expectations.
		// When it doesn't see expected watch events for > TTL, the expectations expire.
		//	- In this case it wakes up, creates/deletes controllees, and sets expectations again.
		// When it has satisfied expectations and no controllees need to be created/destroyed > TTL, the expectations expire.
		//	- In this case it continues without setting expectations till it needs to create/delete controllees.
		klog.V(4).Infof("Cache controller %v either never recorded expectations, or the ttl expired.", controllerKey)
	}
	// Trigger a sync if we either encountered and error (which shouldn't happen since we're
	// getting from local store) or this controller hasn't established expectations.
	return true
}

func (r *CacheExpectations) ExpectCreation(controllerKey string, gvk schema.GroupVersionKind, namespace, name string) error {
	e, err := r.initExpectations(controllerKey)
	if err != nil {
		return err
	}
	klog.V(4).InfoS("Setting expectation for creation", "key", controllerKey, "gvk", gvk, "namespace", namespace, "name", name)
	return e.ExpectCreation(gvk, namespace, name)
}

func (r *CacheExpectations) ExpectDeletion(controllerKey string, gvk schema.GroupVersionKind, namespace, name string) error {
	e, err := r.initExpectations(controllerKey)
	if err != nil {
		return err
	}
	klog.V(4).InfoS("Setting expectation for deletion", "key", controllerKey, "gvk", gvk, "namespace", namespace, "name", name)
	return e.ExpectDeletion(gvk, namespace, name)
}

func (r *CacheExpectations) ExpectUpdation(controllerKey string, gvk schema.GroupVersionKind, namespace, name, resourceVersion string) error {
	e, err := r.initExpectations(controllerKey)
	if err != nil {
		return err
	}
	klog.V(4).InfoS("Setting expectation for updation", "key", controllerKey, "gvk", gvk, "namespace", namespace, "name", name, "rv", resourceVersion)
	return e.ExpectUpdation(gvk, namespace, name, resourceVersion)
}

func (r *CacheExpectations) initExpectations(controllerKey string) (*CacheExpectation, error) {
	e, exists, err := r.GetByKey(controllerKey)
	if err != nil {
		return nil, err
	}
	if !exists {
		e = newCacheExpectation(controllerKey, r.clock, r.reader, r.scheme)
		klog.V(5).InfoS("Creating new cache expectation", "key", controllerKey)
		r.Add(e) // nolint: errcheck
		e, _, _ = r.GetByKey(controllerKey)
	}
	return e.(*CacheExpectation), nil
}

type CacheExpectation struct {
	key string

	clock  clock.Clock
	reader client.Reader
	scheme *runtime.Scheme

	items cache.Store
}

func newCacheExpectation(key string, clock clock.Clock, reader client.Reader, scheme *runtime.Scheme) *CacheExpectation {
	return &CacheExpectation{
		key:    key,
		clock:  clock,
		reader: reader,
		scheme: scheme,
		items:  cache.NewStore(ExpKeyFunc),
	}
}

func (e *CacheExpectation) Key() string {
	return e.key
}

func (e *CacheExpectation) Fulfilled() bool {
	items := e.items.List()
	satisfied := true
	for _, item := range items {
		citem := item.(*CacheExpectationItem)
		if citem.Fulfilled() {
			e.items.Delete(item) // nolint: errcheck
			continue
		}
		satisfied = false
	}
	return satisfied
}

func (e *CacheExpectation) ExpectCreation(gvk schema.GroupVersionKind, namespace, name string) error {
	return e.expect(e.getKey(gvk, namespace, name), e.creationObserved(gvk, namespace, name))
}

func (e *CacheExpectation) ExpectDeletion(gvk schema.GroupVersionKind, namespace, name string) error {
	return e.expect(e.getKey(gvk, namespace, name), e.deletionObserved(gvk, namespace, name))
}

func (e *CacheExpectation) ExpectUpdation(gvk schema.GroupVersionKind, namespace, name, resourceVersion string) error {
	return e.expect(e.getKey(gvk, namespace, name), e.updationObserved(gvk, namespace, name, resourceVersion))
}

func (e *CacheExpectation) expect(key string, fn satisfied) error {
	item, ok, err := e.items.GetByKey(key)
	if err != nil {
		return err
	}
	var eitem *CacheExpectationItem
	if ok {
		eitem = item.(*CacheExpectationItem)
	} else {
		eitem = newCacheExpatationItem(key, e.clock)
		e.items.Add(eitem) // nolint: errcheck
	}
	eitem.Set(fn)
	return nil
}

func (e *CacheExpectation) getKey(gvk schema.GroupVersionKind, namespace, name string) string {
	gk := gvk.GroupKind()
	return fmt.Sprintf("%s/%s/%s", gk.String(), namespace, name)
}

func (e *CacheExpectation) creationObserved(gvk schema.GroupVersionKind, namespace, name string) satisfied {
	return func() bool {
		obj, err := e.scheme.New(gvk)
		if err != nil {
			panic(fmt.Errorf("failed to create new object from schema: %w", err))
		}
		cObj, ok := obj.(client.Object)
		if !ok {
			panic(fmt.Errorf("failed to cast object to client.Object"))
		}
		err = e.reader.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: name}, cObj)
		return err == nil
	}
}

func (e *CacheExpectation) deletionObserved(gvk schema.GroupVersionKind, namespace, name string) satisfied {
	return func() bool {
		obj, err := e.scheme.New(gvk)
		if err != nil {
			panic(fmt.Errorf("failed to create new object from schema: %w", err))
		}
		cObj, ok := obj.(client.Object)
		if !ok {
			panic(fmt.Errorf("failed to cast object to client.Object"))
		}
		err = e.reader.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: name}, cObj)
		if err == nil {
			return false
		}
		return errors.IsNotFound(err)
	}
}

func (e *CacheExpectation) updationObserved(gvk schema.GroupVersionKind, namespace, name, resourceVersion string) satisfied {
	var expectedRV int64
	if resourceVersion == "" {
		expectedRV = 0
	} else {
		var err error
		expectedRV, err = strconv.ParseInt(resourceVersion, 10, 64)
		if err != nil {
			klog.ErrorS(err, "failed to parse resourceVersion to int", "resourceVersion", resourceVersion)
		}
	}

	return func() bool {
		obj, err := e.scheme.New(gvk)
		if err != nil {
			panic(fmt.Errorf("failed to create new object from schema: %w", err))
		}
		cObj, ok := obj.(client.Object)
		if !ok {
			panic(fmt.Errorf("failed to cast object to client.Object"))
		}
		err = e.reader.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: name}, cObj)
		if err != nil {
			return false
		}

		var rv int64
		objRV := cObj.GetResourceVersion()
		if objRV == "" {
			rv = 0
		} else {
			var err error
			rv, err = strconv.ParseInt(objRV, 10, 64)
			if err != nil {
				klog.ErrorS(err, "failed to parse resourceVersion to int", "resourceVersion", objRV)
				return false
			}
		}
		return rv >= expectedRV
	}
}

type CacheExpectationItem struct {
	clock  clock.Clock
	expect atomic.Value

	key       string
	timestamp time.Time
}

type satisfied func() bool

func newCacheExpatationItem(key string, clock clock.Clock) *CacheExpectationItem {
	return &CacheExpectationItem{
		key:   key,
		clock: clock,
	}
}

func (e *CacheExpectationItem) Key() string {
	return e.key
}

func (e *CacheExpectationItem) Set(fn satisfied) {
	e.expect.Store(fn)
	e.timestamp = e.clock.Now()
}

func (e *CacheExpectationItem) Fulfilled() bool {
	expect := e.expect.Load().(satisfied)
	if expect == nil {
		return true
	}
	satisfied := expect()
	if satisfied {
		return true
	}
	if e.isExpired() {
		return true
	}
	return false
}

func (e *CacheExpectationItem) isExpired() bool {
	return e.clock.Since(e.timestamp) > ExpectationsTimeout
}
