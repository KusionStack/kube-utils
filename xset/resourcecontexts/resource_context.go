/*
Copyright 2023-2025 The KusionStack Authors.

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

package resourcecontexts

import (
	"context"
	"fmt"
	"sort"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clientutil "kusionstack.io/kube-utils/client"
	"kusionstack.io/kube-utils/controller/expectations"
	"kusionstack.io/kube-utils/xset/api"
)

type ResourceContextControl interface {
	AllocateID(ctx context.Context, xsetObject api.XSetObject, defaultRevision string, replicas int) (map[int]*api.ContextDetail, error)
	UpdateToTargetContext(ctx context.Context, xsetObject api.XSetObject, ownedIDs map[int]*api.ContextDetail) error
	ExtractAvailableContexts(diff int, ownedIDs map[int]*api.ContextDetail, targetInstanceIDSet sets.Int) []*api.ContextDetail
	GetContextKey(enum api.ResourceContextKeyEnum) string
}

type RealResourceContextControl struct {
	client.Client
	xsetController         api.XSetController
	resourceContextAdapter api.ResourceContextAdapter
	resourceContextKeys    map[api.ResourceContextKeyEnum]string
	resourceContextGVK     schema.GroupVersionKind
	cacheExpectations      expectations.CacheExpectationsInterface
}

func NewRealResourceContextControl(
	c client.Client,
	xsetController api.XSetController,
	resourceContextAdapter api.ResourceContextAdapter,
	resourceContextGVK schema.GroupVersionKind,
	cacheExpectations expectations.CacheExpectationsInterface,
) ResourceContextControl {
	resourceContextKeys := resourceContextAdapter.GetContextKeyManager()
	if resourceContextKeys == nil {
		resourceContextKeys = DefaultResourceContextKeys
	}

	return &RealResourceContextControl{
		Client:                 c,
		xsetController:         xsetController,
		resourceContextAdapter: resourceContextAdapter,
		resourceContextKeys:    resourceContextKeys,
		resourceContextGVK:     resourceContextGVK,
		cacheExpectations:      cacheExpectations,
	}
}

func (r *RealResourceContextControl) AllocateID(
	ctx context.Context,
	xsetObject api.XSetObject,
	defaultRevision string,
	replicas int,
) (map[int]*api.ContextDetail, error) {
	contextName := getContextName(r.xsetController, xsetObject)
	targetContext := r.resourceContextAdapter.NewResourceContext()
	notFound := false
	if err := r.Client.Get(ctx, types.NamespacedName{Namespace: xsetObject.GetNamespace(), Name: contextName}, targetContext); err != nil {
		if !errors.IsNotFound(err) {
			return nil, fmt.Errorf("fail to find ResourceContext %s/%s for owner %s: %s", xsetObject.GetNamespace(), contextName, xsetObject.GetName(), err.Error())
		}

		notFound = true
		targetContext.SetNamespace(xsetObject.GetNamespace())
		targetContext.SetName(contextName)
	}

	xsetSpec := r.xsetController.GetXSetSpec(xsetObject)
	// store all the IDs crossing Multiple workload
	existingIDs := map[int]*api.ContextDetail{}
	// only store the IDs belonging to this owner
	ownedIDs := map[int]*api.ContextDetail{}
	resourceContextSpec := r.resourceContextAdapter.GetResourceContextSpec(targetContext)
	for i := range resourceContextSpec.Contexts {
		detail := &resourceContextSpec.Contexts[i]
		if detail.Contains(r.GetContextKey(api.EnumOwnerContextKey), xsetObject.GetName()) {
			ownedIDs[detail.ID] = detail
			existingIDs[detail.ID] = detail
		} else if xsetSpec.ScaleStrategy.Context != "" {
			// add other collaset targetContexts only if context pool enabled
			existingIDs[detail.ID] = detail
		}
	}

	// if owner has enough ID, return
	if len(ownedIDs) >= replicas {
		return ownedIDs, nil
	}

	// find new IDs for owner
	candidateID := 0
	for len(ownedIDs) < replicas {
		// find one new ID
		for {
			if _, exist := existingIDs[candidateID]; exist {
				candidateID++
				continue
			}

			break
		}

		detail := &api.ContextDetail{
			ID: candidateID,
			// TODO choose just create targets' revision according to scaleStrategy
			Data: map[string]string{
				r.GetContextKey(api.EnumOwnerContextKey):          xsetObject.GetName(),
				r.GetContextKey(api.EnumRevisionContextDataKey):   defaultRevision,
				r.GetContextKey(api.EnumJustCreateContextDataKey): "true",
			},
		}
		existingIDs[candidateID] = detail
		ownedIDs[candidateID] = detail
	}

	if notFound {
		return ownedIDs, r.doCreateTargetContext(ctx, xsetObject, ownedIDs)
	}

	return ownedIDs, r.doUpdateTargetContext(ctx, xsetObject, ownedIDs, targetContext)
}

func (r *RealResourceContextControl) UpdateToTargetContext(
	ctx context.Context,
	xSetObject api.XSetObject,
	ownedIDs map[int]*api.ContextDetail,
) error {
	contextName := getContextName(r.xsetController, xSetObject)
	targetContext := r.resourceContextAdapter.NewResourceContext()
	if err := r.Client.Get(ctx, types.NamespacedName{Namespace: xSetObject.GetNamespace(), Name: contextName}, targetContext); err != nil {
		if !errors.IsNotFound(err) {
			return fmt.Errorf("fail to find ResourceContext %s/%s: %w", xSetObject.GetNamespace(), contextName, err)
		}

		if len(ownedIDs) == 0 {
			return nil
		}

		if err := r.doCreateTargetContext(ctx, xSetObject, ownedIDs); err != nil {
			return fmt.Errorf("fail to create ResourceContext %s/%s after not found: %w", xSetObject.GetNamespace(), contextName, err)
		}
	}

	return r.doUpdateTargetContext(ctx, xSetObject, ownedIDs, targetContext)
}

func (r *RealResourceContextControl) ExtractAvailableContexts(diff int, ownedIDs map[int]*api.ContextDetail, targetInstanceIDSet sets.Int) []*api.ContextDetail {
	var availableContexts []*api.ContextDetail
	if diff <= 0 {
		return availableContexts
	}

	idx := 0
	for id := range ownedIDs {
		if _, inUsed := targetInstanceIDSet[id]; inUsed {
			continue
		}

		availableContexts = append(availableContexts, ownedIDs[id])
		idx++
		if idx == diff {
			break
		}
	}

	return availableContexts
}

func (r *RealResourceContextControl) GetContextKey(enum api.ResourceContextKeyEnum) string {
	return r.resourceContextKeys[enum]
}

func (r *RealResourceContextControl) doCreateTargetContext(
	ctx context.Context,
	xSetObject api.XSetObject,
	ownerIDs map[int]*api.ContextDetail,
) error {
	contextName := getContextName(r.xsetController, xSetObject)
	targetContext := r.resourceContextAdapter.NewResourceContext()
	targetContext.SetNamespace(xSetObject.GetNamespace())
	targetContext.SetName(contextName)

	spec := &api.ResourceContextSpec{}
	for i := range ownerIDs {
		spec.Contexts = append(spec.Contexts, *ownerIDs[i])
	}
	r.resourceContextAdapter.SetResourceContextSpec(spec, targetContext)
	if err := r.Client.Create(ctx, targetContext); err != nil {
		return err
	}
	return r.cacheExpectations.ExpectCreation(clientutil.ObjectKeyString(xSetObject), r.resourceContextGVK, targetContext.GetNamespace(), targetContext.GetName())
}

func (r *RealResourceContextControl) doUpdateTargetContext(
	ctx context.Context,
	xsetObject client.Object,
	ownedIDs map[int]*api.ContextDetail,
	targetContext api.ResourceContextObject,
) error {
	// store all IDs crossing all workload
	existingIDs := map[int]*api.ContextDetail{}

	// add other collaset targetContexts only if context pool enabled
	xsetSpec := r.xsetController.GetXSetSpec(xsetObject)
	resourceContextSpec := r.resourceContextAdapter.GetResourceContextSpec(targetContext)
	ownerContextKey := r.GetContextKey(api.EnumOwnerContextKey)
	if xsetSpec.ScaleStrategy.Context != "" {
		for i := range resourceContextSpec.Contexts {
			detail := resourceContextSpec.Contexts[i]
			if detail.Contains(ownerContextKey, xsetObject.GetName()) {
				continue
			}
			existingIDs[detail.ID] = &detail
		}
	}

	for _, contextDetail := range ownedIDs {
		existingIDs[contextDetail.ID] = contextDetail
	}

	// delete TargetContext if it is empty
	if len(existingIDs) == 0 {
		err := r.Client.Delete(ctx, targetContext)
		if err != nil {
			return err
		}
		return r.cacheExpectations.ExpectDeletion(clientutil.ObjectKeyString(xsetObject), r.resourceContextGVK, targetContext.GetNamespace(), targetContext.GetName())
	}

	resourceContextSpec.Contexts = make([]api.ContextDetail, len(existingIDs))
	idx := 0
	for _, contextDetail := range existingIDs {
		resourceContextSpec.Contexts[idx] = *contextDetail
		idx++
	}

	// keep context detail in order by ID
	sort.Sort(ContextDetailsByOrder(resourceContextSpec.Contexts))
	r.resourceContextAdapter.SetResourceContextSpec(resourceContextSpec, targetContext)
	err := r.Client.Update(ctx, targetContext)
	if err != nil {
		return err
	}
	return r.cacheExpectations.ExpectUpdation(clientutil.ObjectKeyString(xsetObject), r.resourceContextGVK, targetContext.GetNamespace(), targetContext.GetName(), targetContext.GetResourceVersion())
}

func getContextName(xsetControl api.XSetController, instance api.XSetObject) string {
	spec := xsetControl.GetXSetSpec(instance)
	if spec.ScaleStrategy.Context != "" {
		return spec.ScaleStrategy.Context
	}

	return instance.GetName()
}

type ContextDetailsByOrder []api.ContextDetail

func (s ContextDetailsByOrder) Len() int      { return len(s) }
func (s ContextDetailsByOrder) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

func (s ContextDetailsByOrder) Less(i, j int) bool {
	l, r := s[i], s[j]
	return l.ID < r.ID
}
