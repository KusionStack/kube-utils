/*
 * Copyright 2024-2025 KusionStack Authors.
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

package xcontrol

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"kusionstack.io/kube-utils/controller/mixin"
	refmanagerutil "kusionstack.io/kube-utils/controller/refmanager"
	"kusionstack.io/kube-utils/xset/api"
)

const (
	FieldIndexOwnerRefUID = "ownerRefUID"
)

type TargetControl interface {
	GetFilteredTargets(ctx context.Context, selector *metav1.LabelSelector, owner api.XSetObject) ([]client.Object, error)
	CreateTarget(ctx context.Context, target client.Object) (client.Object, error)
	DeleteTarget(ctx context.Context, target client.Object) error
	UpdateTarget(ctx context.Context, target client.Object) error
	PatchTarget(ctx context.Context, target client.Object, patch client.Patch) error
	OrphanTarget(xset api.XSetObject, target client.Object) error
	AdoptTarget(xset api.XSetObject, target client.Object) error
}

type targetControl struct {
	client client.Client
	schema *runtime.Scheme

	xsetController api.XSetController
	xGVK           schema.GroupVersionKind
}

func NewTargetControl(mixin *mixin.ReconcilerMixin, xsetController api.XSetController) (TargetControl, error) {
	if err := setUpCache(mixin.Cache, xsetController); err != nil {
		return nil, err
	}

	xMeta := xsetController.XMeta()
	gvk := xMeta.GroupVersionKind()
	return &targetControl{
		client:         mixin.Client,
		schema:         mixin.Scheme,
		xsetController: xsetController,
		xGVK:           gvk,
	}, nil
}

func (r *targetControl) GetFilteredTargets(ctx context.Context, selector *metav1.LabelSelector, owner api.XSetObject) ([]client.Object, error) {
	targetList := r.xsetController.NewXObjectList()
	if err := r.client.List(ctx, targetList, &client.ListOptions{
		Namespace:     owner.GetNamespace(),
		FieldSelector: fields.OneTermEqualSelector(FieldIndexOwnerRefUID, string(owner.GetUID())),
	}); err != nil {
		return nil, err
	}

	targetListVal := reflect.Indirect(reflect.ValueOf(targetList))
	itemsVal := targetListVal.FieldByName("Items")
	if !itemsVal.IsValid() {
		return nil, fmt.Errorf("target list items is invalid")
	}

	var items []client.Object
	if itemsVal.Kind() == reflect.Slice {
		items = make([]client.Object, itemsVal.Len())
		for i := 0; i < itemsVal.Len(); i++ {
			itemVal := itemsVal.Index(i).Addr().Interface()
			items[i] = itemVal.(client.Object)
		}
	} else {
		return nil, fmt.Errorf("target list items is invalid")
	}

	// todo filterOutInactiveTargets
	targets, err := r.getTargets(items, selector, owner)
	return targets, err
}

func (r *targetControl) CreateTarget(ctx context.Context, target client.Object) (client.Object, error) {
	if err := r.client.Create(ctx, target); err != nil {
		return nil, fmt.Errorf("failed to create target: %s", err.Error())
	}
	return target, nil
}

func (r *targetControl) DeleteTarget(ctx context.Context, target client.Object) error {
	return r.client.Delete(ctx, target)
}

func (r *targetControl) UpdateTarget(ctx context.Context, target client.Object) error {
	return r.client.Update(ctx, target)
}

func (r *targetControl) PatchTarget(ctx context.Context, target client.Object, patch client.Patch) error {
	return r.client.Patch(ctx, target, patch)
}

func (r *targetControl) OrphanTarget(xset api.XSetObject, target client.Object) error {
	spec := r.xsetController.GetXSetSpec(xset)
	if spec.Selector.MatchLabels == nil {
		return nil
	}

	if target.GetLabels() == nil {
		target.SetLabels(make(map[string]string))
	}
	if target.GetAnnotations() == nil {
		target.SetAnnotations(make(map[string]string))
	}

	refWriter := refmanagerutil.NewOwnerRefWriter(r.client)
	if err := refWriter.Release(context.TODO(), xset, target); err != nil {
		return fmt.Errorf("failed to orphan target: %s", err.Error())
	}

	return nil
}

func (r *targetControl) AdoptTarget(xset api.XSetObject, target client.Object) error {
	spec := r.xsetController.GetXSetSpec(xset)
	if spec.Selector.MatchLabels == nil {
		return nil
	}

	refWriter := refmanagerutil.NewOwnerRefWriter(r.client)
	matcher, err := refmanagerutil.LabelSelectorAsMatch(spec.Selector)
	if err != nil {
		return fmt.Errorf("fail to create labelSelector matcher: %s", err.Error())
	}
	refManager := refmanagerutil.NewObjectControllerRefManager(refWriter, xset, xset.GetObjectKind().GroupVersionKind(), matcher)

	if _, err = refManager.Claim(context.TODO(), target); err != nil {
		return fmt.Errorf("failed to adopt target: %s", err.Error())
	}

	return nil
}

func (r *targetControl) getTargets(candidates []client.Object, selector *metav1.LabelSelector, xset api.XSetObject) ([]client.Object, error) {
	// Use RefManager to adopt/orphan as needed.
	writer := refmanagerutil.NewOwnerRefWriter(r.client)
	matcher, err := refmanagerutil.LabelSelectorAsMatch(selector)
	if err != nil {
		return nil, fmt.Errorf("fail to create labelSelector matcher: %s", err.Error())
	}
	refManager := refmanagerutil.NewObjectControllerRefManager(writer, xset, xset.GetObjectKind().GroupVersionKind(), matcher)

	var claimObjs []client.Object
	var errList []error
	for _, obj := range candidates {
		ok, err := refManager.Claim(context.TODO(), obj)
		if err != nil {
			errList = append(errList, err)
			continue
		}
		if ok {
			claimObjs = append(claimObjs, obj)
		}
	}

	return claimObjs, errors.Join(errList...)
}

func setUpCache(cache cache.Cache, controller api.XSetController) error {
	if err := cache.IndexField(context.TODO(), controller.NewXObject(), FieldIndexOwnerRefUID, func(object client.Object) []string {
		ownerRef := metav1.GetControllerOf(object)
		if ownerRef == nil || ownerRef.Kind != controller.XSetMeta().Kind {
			return nil
		}
		return []string{string(ownerRef.UID)}
	}); err != nil {
		return fmt.Errorf("failed to index by field %s: %s", FieldIndexOwnerRefUID, err.Error())
	}
	return nil
}
