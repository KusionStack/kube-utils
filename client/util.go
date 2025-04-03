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

// UpdateOnConflict attempts to update a resource while avoiding conflicts that may arise from concurrent modifications.
// It utilizes the mutateFn function to apply changes to the original obj and then attempts an update using the writer,
// which can be either client.Writer or client.StatusWriter.
// In case of an update failure due to a conflict, UpdateOnConflict will retrieve the latest version of the object using
// the reader and attempt the update again.
// The retry mechanism adheres to the retry.DefaultBackoff policy.
func UpdateOnConflict[T client.Object](
	ctx context.Context,
	reader client.Reader,
	writer UpdateWriter,
	original T,
	mutateFn MutateFn[T],
) (changed bool, err error) {
	key := client.ObjectKeyFromObject(original)
	first := true

	err = retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		if !first {
			// refresh object
			if innerErr := reader.Get(ctx, key, original); innerErr != nil {
				return innerErr
			}
		} else {
			first = false
		}

		existing := original.DeepCopyObject()
		if innerErr := mutate(mutateFn, key, original); innerErr != nil {
			return innerErr
		}

		if equality.Semantic.DeepEqual(existing, original) {
			// nothing changed, skip update
			return nil
		}

		if innerErr := writer.Update(ctx, original); innerErr != nil {
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
	original T,
	f MutateFn[T],
) (controllerutil.OperationResult, error) {
	key := client.ObjectKeyFromObject(original)
	if err := reader.Get(ctx, key, original); err != nil {
		if !apierrors.IsNotFound(err) {
			return controllerutil.OperationResultNone, err
		}
		// not found, create it
		if err := mutate(f, key, original); err != nil {
			return controllerutil.OperationResultNone, err
		}
		if err := writer.Create(ctx, original); err != nil {
			return controllerutil.OperationResultNone, err
		}
		return controllerutil.OperationResultCreated, nil
	}
	changed, err := UpdateOnConflict(ctx, reader, writer, original, f)
	if err != nil {
		return controllerutil.OperationResultNone, err
	}
	if !changed {
		return controllerutil.OperationResultNone, nil
	}
	return controllerutil.OperationResultUpdated, nil
}
