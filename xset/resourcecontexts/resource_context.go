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

func AllocateID(xsetControl api.XSetController, c client.Client, e *expectations.CacheExpectation,
	instance api.XSetObject, defaultRevision string, replicas int,
) (map[int]*appsv1alpha1.ContextDetail, error) {
	contextName := getContextName(xsetControl, instance)
	targetContext := &appsv1alpha1.ResourceContext{}
	notFound := false
	if err := c.Get(context.TODO(), types.NamespacedName{Namespace: instance.GetNamespace(), Name: contextName}, targetContext); err != nil {
		if !errors.IsNotFound(err) {
			return nil, fmt.Errorf("fail to find ResourceContext %s/%s for owner %s: %s", instance.GetNamespace(), contextName, instance.GetName(), err.Error())
		}

		notFound = true
		targetContext.Namespace = instance.GetNamespace()
		targetContext.Name = contextName
	}

	xspec := xsetControl.GetXSetSpec(instance)
	// store all the IDs crossing Multiple workload
	existingIDs := map[int]*appsv1alpha1.ContextDetail{}
	// only store the IDs belonging to this owner
	ownedIDs := map[int]*appsv1alpha1.ContextDetail{}
	for i := range targetContext.Spec.Contexts {
		detail := &targetContext.Spec.Contexts[i]
		if detail.Contains(OwnerContextKey, instance.GetName()) {
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
				OwnerContextKey:          instance.GetName(),
				RevisionContextDataKey:   defaultRevision,
				JustCreateContextDataKey: "true",
			},
		}
		existingIDs[candidateID] = detail
		ownedIDs[candidateID] = detail
	}

	if notFound {
		return ownedIDs, doCreateTargetContext(xsetControl, c, instance, ownedIDs)
	}

	return ownedIDs, doUpdateTargetContext(xsetControl, c, e, instance, ownedIDs, targetContext)
}

func UpdateToTargetContext(xsetController api.XSetController, c client.Client, e *expectations.CacheExpectation,
	instance api.XSetObject, ownedIDs map[int]*appsv1alpha1.ContextDetail,
) error {
	contextName := getContextName(xsetController, instance)
	targetContext := &appsv1alpha1.ResourceContext{}
	if err := c.Get(context.TODO(), types.NamespacedName{Namespace: instance.GetNamespace(), Name: contextName}, targetContext); err != nil {
		if !errors.IsNotFound(err) {
			return fmt.Errorf("fail to find ResourceContext %s/%s: %w", instance.GetNamespace(), contextName, err)
		}

		if len(ownedIDs) == 0 {
			return nil
		}

		if err := doCreateTargetContext(xsetController, c, instance, ownedIDs); err != nil {
			return fmt.Errorf("fail to create ResourceContext %s/%s after not found: %w", instance.GetNamespace(), contextName, err)
		}
	}

	return doUpdateTargetContext(xsetController, c, e, instance, ownedIDs, targetContext)
}

func ExtractAvailableContexts(diff int, ownedIDs map[int]*appsv1alpha1.ContextDetail, targetInstanceIDSet sets.Int) []*appsv1alpha1.ContextDetail {
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

func doCreateTargetContext(xsetController api.XSetController, c client.Client, instance api.XSetObject, ownerIDs map[int]*appsv1alpha1.ContextDetail) error {
	contextName := getContextName(xsetController, instance)
	targetContext := &appsv1alpha1.ResourceContext{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: instance.GetNamespace(),
			Name:      contextName,
		},
		Spec: appsv1alpha1.ResourceContextSpec{
			Contexts: make([]appsv1alpha1.ContextDetail, len(ownerIDs)),
		},
	}

	setContextData(ownerIDs, targetContext)
	return c.Create(context.TODO(), targetContext)
}

func doUpdateTargetContext(xsetController api.XSetController, c client.Client, e *expectations.CacheExpectation,
	instance client.Object, ownedIDs map[int]*appsv1alpha1.ContextDetail, targetContext *appsv1alpha1.ResourceContext,
) error {
	// store all IDs crossing all workload
	existingIDs := map[int]*appsv1alpha1.ContextDetail{}

	// add other collaset targetContexts only if context pool enabled
	spec := xsetController.GetXSetSpec(instance)
	if spec.ScaleStrategy.Context != "" {
		for i := range targetContext.Spec.Contexts {
			detail := targetContext.Spec.Contexts[i]
			if detail.Contains(OwnerContextKey, instance.GetName()) {
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
		err := c.Delete(context.TODO(), targetContext)
		if err != nil {
			if err := e.ExpectDeletion(resourceContextGVK, targetContext.Namespace, targetContext.Name); err != nil {
				return err
			}
		}

		return err
	}

	setContextData(existingIDs, targetContext)
	err := c.Update(context.TODO(), targetContext)
	if err != nil {
		if err := e.ExpectUpdation(resourceContextGVK, targetContext.Namespace, targetContext.Name, targetContext.ResourceVersion); err != nil {
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
