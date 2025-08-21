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

import "kusionstack.io/kube-utils/xset/api"

var defaultResourceContextKey = map[api.ResourceContextKeyEnum]string{
	api.EnumOwnerContextKey:              OwnerContextKey,
	api.EnumRevisionContextDataKey:       RevisionContextDataKey,
	api.EnumTargetDecorationRevisionKey:  TargetDecorationRevisionKey,
	api.EnumJustCreateContextDataKey:     JustCreateContextDataKey,
	api.EnumRecreateUpdateContextDataKey: RecreateUpdateContextDataKey,
	api.EnumScaleInContextDataKey:        ScaleInContextDataKey,
}

func NewResourceContextKeyManager() api.ResourceContextKeyManager {
	return &resourceContextKeyManager{
		m: defaultResourceContextKey,
	}
}

type resourceContextKeyManager struct {
	m map[api.ResourceContextKeyEnum]string
}

func (r *resourceContextKeyManager) Get(key api.ResourceContextKeyEnum) string {
	return r.m[key]
}
