/*
Copyright 2025 The KusionStack Authors.

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

package client

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// MutateFn is a function which mutates the existing object into it's desired state.
type MutateFn[T client.Object] func(obj T) error

type UpdateWriter interface {
	// Update updates the fields corresponding to the status subresource for the
	// given obj. obj must be a struct pointer so that obj can be updated
	// with the content returned by the Server.
	Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error
}

// UpdateOnConflict attempts to update a resource while avoiding conflicts that may arise
// from concurrent modifications. It utilizes the mutateFn function to apply changes to the
// input obj and then attempts an update using the writer, which can be either client.Writer
// or client.StatusWriter.
//
// In case of an update failure due to a conflict, UpdateOnConflict will retrieve the latest version
// of the object using the reader and attempt the update again. The retry mechanism adheres to the
// retry.DefaultBackoff policy.
//
// NOTE: It must be ensured that the input object has not been modified in any way after being obtained
// from cache or apiserver.
func UpdateOnConflict[T client.Object](
	ctx context.Context,
	reader client.Reader,
	writer UpdateWriter,
	inputObj T,
	mutateFn MutateFn[T],
) (changed bool, err error) {
	key := client.ObjectKeyFromObject(inputObj)
	first := true

	err = retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		if !first {
			// refresh object
			if innerErr := reader.Get(ctx, key, inputObj); innerErr != nil {
				return innerErr
			}
		} else {
			first = false
		}

		original := inputObj.DeepCopyObject()
		if innerErr := mutate(mutateFn, key, inputObj); innerErr != nil {
			return innerErr
		}

		if equality.Semantic.DeepEqual(original, inputObj) {
			// nothing changed, skip update
			return nil
		}

		if innerErr := writer.Update(ctx, inputObj); innerErr != nil {
			return innerErr
		}

		changed = true
		return nil
	})

	return changed, err
}

// mutate wraps a MutateFn and applies validation to its result.
func mutate[T client.Object](f MutateFn[T], key client.ObjectKey, obj T) error {
	if err := f(obj); err != nil {
		return err
	}
	if newKey := client.ObjectKeyFromObject(obj); key != newKey {
		return fmt.Errorf("MutateFn cannot mutate object name and/or object namespace")
	}
	return nil
}

// CreateOrUpdateOnConflict creates or updates the given object in the Kubernetes
// cluster. The object's desired state must be reconciled with the existing
// state inside the passed in callback MutateFn.
//
// The MutateFn is called regardless of creating or updating an object.
//
// It returns the executed operation and an error.
func CreateOrUpdateOnConflict[T client.Object](
	ctx context.Context,
	reader client.Reader,
	writer client.Writer,
	inputObj T,
	f MutateFn[T],
) (controllerutil.OperationResult, error) {
	key := client.ObjectKeyFromObject(inputObj)
	if err := reader.Get(ctx, key, inputObj); err != nil {
		if !apierrors.IsNotFound(err) {
			return controllerutil.OperationResultNone, err
		}
		// not found, create it
		if err := mutate(f, key, inputObj); err != nil {
			return controllerutil.OperationResultNone, err
		}
		if err := writer.Create(ctx, inputObj); err != nil {
			return controllerutil.OperationResultNone, err
		}
		return controllerutil.OperationResultCreated, nil
	}
	changed, err := UpdateOnConflict(ctx, reader, writer, inputObj, f)
	if err != nil {
		return controllerutil.OperationResultNone, err
	}
	if !changed {
		return controllerutil.OperationResultNone, nil
	}
	return controllerutil.OperationResultUpdated, nil
}

type PatchWriter interface {
	// Patch patches the given obj in the Kubernetes cluster. obj must be a
	// struct pointer so that obj can be updated with the content returned by the Server.
	Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error
}

// Patch applies changes to a Kubernetes object using the merge patch strategy.
// It creates a merge patch from the original object, applies the mutateFn to modify the object,
// and then applies the patch if there are actual changes.
// Returns true if changes were applied, false otherwise.
func Patch[T client.Object](
	ctx context.Context,
	writer PatchWriter,
	inputObj T,
	mutateFn MutateFn[T],
) (changed bool, err error) {
	original := inputObj.DeepCopyObject().(client.Object)

	mergePatch := client.MergeFrom(original)

	// modify inputObj object
	err = mutateFn(inputObj)
	if err != nil {
		return false, err
	}

	// calculate patch data
	patchData, err := mergePatch.Data(inputObj)
	if err != nil {
		return false, err
	}

	if len(patchData) == 0 || string(patchData) == "{}" {
		// nothing changed, skip update
		return false, nil
	}

	err = writer.Patch(ctx, inputObj, mergePatch)
	if err != nil {
		return false, err
	}

	// patched
	return true, nil
}

// PatchOnConflict attempts to patch a resource while avoiding conflicts that may arise
// from concurrent modifications. It utilizes the mutateFn function to apply changes to the
// input obj and then attempts an mergePatch using the writer, which can be either client.Writer
// or client.StatusWriter.
//
// In case of an update failure due to a conflict, PatchOnConflict will retrieve the latest version
// of the object using the reader and attempt the update again. The retry mechanism adheres to the
// retry.DefaultBackoff policy.
//
// NOTE: It must be ensured that the input object has not been modified in any way after being obtained
// from cache or apiserver.
func PatchOnConflict[T client.Object](
	ctx context.Context,
	reader client.Reader,
	writer PatchWriter,
	inputObj T,
	mutateFn MutateFn[T],
) (changed bool, err error) {
	key := client.ObjectKeyFromObject(inputObj)
	first := true

	err = retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		if !first {
			// refresh object
			if innerErr := reader.Get(ctx, key, inputObj); innerErr != nil {
				return innerErr
			}
		} else {
			first = false
		}

		original := inputObj.DeepCopyObject().(client.Object)

		// modify inputObj object
		if innerErr := mutateFn(inputObj); innerErr != nil {
			return innerErr
		}

		mergePatch := client.MergeFrom(original)

		// calculate patch data
		patchData, err := mergePatch.Data(inputObj)
		if err != nil {
			return err
		}

		if len(patchData) == 0 || string(patchData) == "{}" {
			// nothing changed, skip update
			return nil
		}

		// use resourceVersion as optimisticLock
		mergePatchWithOptimisticLock := client.MergeFromWithOptions(original, client.MergeFromWithOptimisticLock{})
		if innerErr := writer.Patch(ctx, inputObj, mergePatchWithOptimisticLock); innerErr != nil {
			return innerErr
		}

		changed = true
		return nil
	})

	return changed, err
}
