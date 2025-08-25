/*
 * Copyright 2024 - 2025 KusionStack Authors.
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

package resourcecontexts

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	appsv1alpha1 "kusionstack.io/kube-api/apps/v1alpha1"

	"kusionstack.io/kube-utils/xset/api"
)

var _ api.ResourceContextAdapter = &DefaultResourceContextAdapter{}

var defaultResourceContextKeys = map[api.ResourceContextKeyEnum]string{
	api.EnumOwnerContextKey:                     "Owner",
	api.EnumRevisionContextDataKey:              "Revision",
	api.EnumTargetDecorationRevisionKey:         "TargetDecorationRevisions",
	api.EnumJustCreateContextDataKey:            "TargetJustCreate",
	api.EnumRecreateUpdateContextDataKey:        "TargetRecreateUpdate",
	api.EnumScaleInContextDataKey:               "ScaleIn",
	api.EnumReplaceNewTargetIDContextDataKey:    "ReplaceNewTargetID",
	api.EnumReplaceOriginTargetIDContextDataKey: "ReplaceOriginTargetID",
}

type ResourceContextAdapterGetter struct {
}

func (r *ResourceContextAdapterGetter) GetResourceContextAdapter() api.ResourceContextAdapter {
	return &DefaultResourceContextAdapter{}
}

// DefaultResourceContextAdapter is the adapter to api apps.kusionstack.io.resourcecontexts
type DefaultResourceContextAdapter struct{}

func (*DefaultResourceContextAdapter) ResourceContextMeta() metav1.TypeMeta {
	return metav1.TypeMeta{APIVersion: appsv1alpha1.SchemeGroupVersion.String(), Kind: "ResourceContext"}
}

func (*DefaultResourceContextAdapter) GetResourceContextSpec(object api.ResourceContextObject) *api.ResourceContextSpec {
	rc := object.(*appsv1alpha1.ResourceContext)
	var contexts []api.ContextDetail
	for i := range rc.Spec.Contexts {
		c := rc.Spec.Contexts[i]
		contexts = append(contexts, api.ContextDetail{
			ID:   c.ID,
			Data: c.Data,
		})
	}
	return &api.ResourceContextSpec{
		Contexts: contexts,
	}
}

func (*DefaultResourceContextAdapter) SetResourceContextSpec(spec *api.ResourceContextSpec, object api.ResourceContextObject) {
	rc := object.(*appsv1alpha1.ResourceContext)
	var contexts []appsv1alpha1.ContextDetail
	for i := range spec.Contexts {
		c := spec.Contexts[i]
		contexts = append(contexts, appsv1alpha1.ContextDetail{
			ID:   c.ID,
			Data: c.Data,
		})
	}
	rc.Spec.Contexts = contexts
}

func (*DefaultResourceContextAdapter) GetContextKeys() map[api.ResourceContextKeyEnum]string {
	return defaultResourceContextKeys
}

func (*DefaultResourceContextAdapter) NewResourceContext() api.ResourceContextObject {
	return &appsv1alpha1.ResourceContext{}
}
