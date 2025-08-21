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

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	appsv1alpha1 "kusionstack.io/kube-api/apps/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clientutil "kusionstack.io/kube-utils/client"
	"kusionstack.io/kube-utils/controller/expectations"
	"kusionstack.io/kube-utils/xset/api"
)

var resourceContextGVK = appsv1alpha1.SchemeGroupVersion.WithKind("ResourceContext")

const (
	OwnerContextKey              = "Owner"
	RevisionContextDataKey       = "Revision"
	TargetDecorationRevisionKey  = "TargetDecorationRevisions"
	JustCreateContextDataKey     = "TargetJustCreate"
	RecreateUpdateContextDataKey = "TargetRecreateUpdate"
	ScaleInContextDataKey        = "ScaleIn"
)

type ResourceContext interface {
	AllocateID(ctx context.Context, xsetControl api.XSetController, xsetObject api.XSetObject, defaultRevision string, replicas int) (map[int]*appsv1alpha1.ContextDetail, error)
	UpdateToTargetContext(ctx context.Context, xsetController api.XSetController, xsetObject api.XSetObject, ownedIDs map[int]*appsv1alpha1.ContextDetail) error
	ExtractAvailableContexts(diff int, ownedIDs map[int]*appsv1alpha1.ContextDetail, targetInstanceIDSet sets.Int) []*appsv1alpha1.ContextDetail
}

type RealResourceContext struct {
	client.Client
	cacheExpectations expectations.CacheExpectationsInterface
}

func NewRealResourceContext(c client.Client, cacheExpectations expectations.CacheExpectationsInterface) ResourceContext {
	return &RealResourceContext{
		Client:            c,
		cacheExpectations: cacheExpectations,
	}
}

func (r *RealResourceContext) AllocateID(ctx context.Context, xsetControl api.XSetController, xsetObject api.XSetObject, defaultRevision string, replicas int,
) (map[int]*appsv1alpha1.ContextDetail, error) {
	contextName := getContextName(xsetControl, xsetObject)
	targetContext := &appsv1alpha1.ResourceContext{}
	notFound := false
	if err := r.Client.Get(ctx, types.NamespacedName{Namespace: xsetObject.GetNamespace(), Name: contextName}, targetContext); err != nil {
		if !errors.IsNotFound(err) {
			return nil, fmt.Errorf("fail to find ResourceContext %s/%s for owner %s: %s", xsetObject.GetNamespace(), contextName, xsetObject.GetName(), err.Error())
		}

		notFound = true
		targetContext.Namespace = xsetObject.GetNamespace()
		targetContext.Name = contextName
	}

	xspec := xsetControl.GetXSetSpec(xsetObject)
	// store all the IDs crossing Multiple workload
	existingIDs := map[int]*appsv1alpha1.ContextDetail{}
	// only store the IDs belonging to this owner
	ownedIDs := map[int]*appsv1alpha1.ContextDetail{}
	for i := range targetContext.Spec.Contexts {
		detail := &targetContext.Spec.Contexts[i]
		if detail.Contains(OwnerContextKey, xsetObject.GetName()) {
			ownedIDs[detail.ID] = detail
			existingIDs[detail.ID] = detail
		} else if xspec.ScaleStrategy.Context != "" {
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

		detail := &appsv1alpha1.ContextDetail{
			ID: candidateID,
			// TODO choose just create targets' revision according to scaleStrategy
			Data: map[string]string{
				OwnerContextKey:          xsetObject.GetName(),
				RevisionContextDataKey:   defaultRevision,
				JustCreateContextDataKey: "true",
			},
		}
		existingIDs[candidateID] = detail
		ownedIDs[candidateID] = detail
	}

	if notFound {
		return ownedIDs, r.doCreateTargetContext(ctx, xsetControl, xsetObject, ownedIDs)
	}

	return ownedIDs, r.doUpdateTargetContext(ctx, xsetControl, xsetObject, ownedIDs, targetContext)
}

func (r *RealResourceContext) UpdateToTargetContext(ctx context.Context, xsetController api.XSetController, xSetObject api.XSetObject, ownedIDs map[int]*appsv1alpha1.ContextDetail,
) error {
	contextName := getContextName(xsetController, xSetObject)
	targetContext := &appsv1alpha1.ResourceContext{}
	if err := r.Client.Get(ctx, types.NamespacedName{Namespace: xSetObject.GetNamespace(), Name: contextName}, targetContext); err != nil {
		if !errors.IsNotFound(err) {
			return fmt.Errorf("fail to find ResourceContext %s/%s: %w", xSetObject.GetNamespace(), contextName, err)
		}

		if len(ownedIDs) == 0 {
			return nil
		}

		if err := r.doCreateTargetContext(ctx, xsetController, xSetObject, ownedIDs); err != nil {
			return fmt.Errorf("fail to create ResourceContext %s/%s after not found: %w", xSetObject.GetNamespace(), contextName, err)
		}
	}

	return r.doUpdateTargetContext(ctx, xsetController, xSetObject, ownedIDs, targetContext)
}

func (r *RealResourceContext) ExtractAvailableContexts(diff int, ownedIDs map[int]*appsv1alpha1.ContextDetail, targetInstanceIDSet sets.Int) []*appsv1alpha1.ContextDetail {
	var availableContexts []*appsv1alpha1.ContextDetail
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

func (r *RealResourceContext) doCreateTargetContext(ctx context.Context, xsetController api.XSetController, xSetObject api.XSetObject, ownerIDs map[int]*appsv1alpha1.ContextDetail) error {
	contextName := getContextName(xsetController, xSetObject)
	targetContext := &appsv1alpha1.ResourceContext{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: xSetObject.GetNamespace(),
			Name:      contextName,
		},
		Spec: appsv1alpha1.ResourceContextSpec{
			Contexts: make([]appsv1alpha1.ContextDetail, len(ownerIDs)),
		},
	}

	setContextData(ownerIDs, targetContext)
	return r.Client.Create(ctx, targetContext)
}

func (r *RealResourceContext) doUpdateTargetContext(ctx context.Context, xsetController api.XSetController, xsetObject client.Object, ownedIDs map[int]*appsv1alpha1.ContextDetail, targetContext *appsv1alpha1.ResourceContext,
) error {
	// store all IDs crossing all workload
	existingIDs := map[int]*appsv1alpha1.ContextDetail{}

	// add other collaset targetContexts only if context pool enabled
	spec := xsetController.GetXSetSpec(xsetObject)
	if spec.ScaleStrategy.Context != "" {
		for i := range targetContext.Spec.Contexts {
			detail := targetContext.Spec.Contexts[i]
			if detail.Contains(OwnerContextKey, xsetObject.GetName()) {
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
			if err := r.cacheExpectations.ExpectDeletion(clientutil.ObjectKeyString(xsetObject), resourceContextGVK, targetContext.Namespace, targetContext.Name); err != nil {
				return err
			}
		}

		return err
	}

	setContextData(existingIDs, targetContext)
	err := r.Client.Update(ctx, targetContext)
	if err != nil {
		if err := r.cacheExpectations.ExpectUpdation(clientutil.ObjectKeyString(xsetObject), resourceContextGVK, targetContext.Namespace, targetContext.Name, targetContext.ResourceVersion); err != nil {
			return err
		}
	}

	return err
}

func getContextName(xsetControl api.XSetController, instance api.XSetObject) string {
	spec := xsetControl.GetXSetSpec(instance)
	if spec.ScaleStrategy.Context != "" {
		return spec.ScaleStrategy.Context
	}

	return instance.GetName()
}

func setContextData(detailIDs map[int]*appsv1alpha1.ContextDetail, targetContext *appsv1alpha1.ResourceContext) {
	length := len(detailIDs)
	targetContext.Spec.Contexts = make([]appsv1alpha1.ContextDetail, 0, length)
	idx := 0
	for len(targetContext.Spec.Contexts) < length {
		for _, ok := detailIDs[idx]; !ok; {
			idx += 1
			_, ok = detailIDs[idx]
		}
		targetContext.Spec.Contexts = append(targetContext.Spec.Contexts, *detailIDs[idx])
		idx += 1
	}
}
