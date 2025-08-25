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

package validation

import (
	"errors"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"kusionstack.io/kube-utils/xset/api"
)

// ValidateResourceContextAdapter validates the resource context adapter
func ValidateResourceContextAdapter(adapter api.ResourceContextAdapter) error {
	if adapter == nil {
		return errors.New("resource context adapter is nil")
	}
	var errList []error
	errList = append(errList,
		validateResourceContextMeta(adapter.ResourceContextMeta()),
		validateResourceContextKey(adapter.GetContextKeys()))
	return errors.Join(errList...)
}

func validateResourceContextMeta(t metav1.TypeMeta) error {
	if len(t.String()) == 0 {
		return errors.New("resource context meta is not valid")
	}
	return nil
}

func validateResourceContextKey(m map[api.ResourceContextKeyEnum]string) error {
	if m == nil {
		return errors.New("resource context keys is nil")
	}

	for i := range api.EnumContextKeyNum {
		if _, ok := m[api.ResourceContextKeyEnum(i)]; !ok {
			return errors.New("resource context keys is not valid, please add enough context keys")
		}
	}
	return nil
}
