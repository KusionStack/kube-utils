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
	"fmt"

	"k8s.io/apimachinery/pkg/util/validation"

	"kusionstack.io/kube-utils/xset/api"
)

// ValidateXSetController validates the XSetController
func ValidateXSetController(xSetController api.XSetController) error {
	if xSetController == nil {
		return errors.New("xSetController is nil")
	}
	if _, ok := xSetController.(api.XSetOperation); !ok {
		return errors.New("XSetOperation is not implemented")
	}
	if _, ok := xSetController.(api.XOperation); !ok {
		return errors.New("XOperation is not implemented")
	}
	return errors.Join(
		validateMeta(xSetController.XSetMeta()),
		validateMeta(xSetController.XMeta()),
		validateFinalizerName(xSetController.FinalizerName()),
		validateXSetLabelAnnotationManager(xSetController),
	)
}

func validateFinalizerName(name string) error {
	msg := validation.IsQualifiedName(name)
	if len(msg) > 0 {
		return errors.New(fmt.Sprintf("%v", msg))
	}
	return nil
}

func validateXSetLabelAnnotationManager(xSetController api.XSetController) error {
	var manager api.XSetLabelAnnotationManager
	if getter, ok := xSetController.(api.LabelAnnotationManagerGetter); ok {
		manager = getter.GetLabelManagerAdapter()
	} else {
		manager = api.NewXSetLabelAnnotationManager()
	}
	if manager == nil {
		return nil
	}

	for i := range api.EnumXSetLabelAnnotationsNum {
		if len(manager.Value(api.XSetLabelAnnotationEnum(i))) == 0 {
			return errors.New("XSetLabelAnnotationManager label annotations are not valid, please add enough label annotations")
		}
	}
	return nil
}
