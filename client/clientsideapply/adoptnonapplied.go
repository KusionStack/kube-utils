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

package clientsideapply

import (
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/endpoints/handlers/fieldmanager"
)

type adoptNonAppliedManager struct {
	fieldManager  fieldmanager.Manager
	objectCreater runtime.ObjectCreater
	gvk           schema.GroupVersionKind
}

var _ fieldmanager.Manager = &adoptNonAppliedManager{}

// NewAdoptNonAppliedManager creates a new wrapped FieldManager that only starts tracking managers after the first apply.
//
// This FieldManager behavior is the opposite of the other FieldManager (fieldmanager.SkipNonAppliedManager).
// It will let the apply field manager take over all fields on liveObj before first apply.
func NewAdoptNonAppliedManager(fieldManager fieldmanager.Manager, objectCreater runtime.ObjectCreater, gvk schema.GroupVersionKind) fieldmanager.Manager {
	return &adoptNonAppliedManager{
		fieldManager:  fieldManager,
		objectCreater: objectCreater,
		gvk:           gvk,
	}
}

// Update implements Manager.
func (f *adoptNonAppliedManager) Update(liveObj, newObj runtime.Object, managed fieldmanager.Managed, manager string) (runtime.Object, fieldmanager.Managed, error) {
	return f.fieldManager.Update(liveObj, newObj, managed, manager)
}

// Apply implements Manager.
func (f *adoptNonAppliedManager) Apply(liveObj, appliedObj runtime.Object, managed fieldmanager.Managed, fieldManager string, force bool) (runtime.Object, fieldmanager.Managed, error) {
	if len(managed.Fields()) == 0 {
		emptyObj, err := f.objectCreater.New(f.gvk)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create empty object of type %v: %w", f.gvk, err)
		}
		liveObj, managed, err = f.fieldManager.Apply(emptyObj, liveObj, managed, fieldManager, force)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create manager for existing fields: %w", err)
		}
	}
	return f.fieldManager.Apply(liveObj, appliedObj, managed, fieldManager, force)
}
