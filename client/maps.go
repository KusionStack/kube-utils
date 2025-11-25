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

import "sigs.k8s.io/controller-runtime/pkg/client"

func MutateLabels(obj client.Object, mutateFn func(map[string]string)) {
	if mutateFn == nil {
		return
	}
	labels := obj.GetLabels()
	labels = mutateMapStrings(labels, mutateFn)
	obj.SetLabels(labels)
}

func MutateAnnotations(obj client.Object, mutateFn func(map[string]string)) {
	if mutateFn == nil {
		return
	}

	annotations := obj.GetAnnotations()
	annotations = mutateMapStrings(annotations, mutateFn)
	obj.SetAnnotations(annotations)
}

func mutateMapStrings(in map[string]string, mutateFn func(map[string]string)) map[string]string {
	var mapIsNil bool
	if in == nil {
		in = make(map[string]string)
		mapIsNil = true
	}
	mutateFn(in)
	if mapIsNil && len(in) == 0 {
		// keep map nil if it's empty
		in = nil
	}

	return in
}

func GetMapValueByDefault[K comparable, V any](m map[K]V, key K, defaultValue V) V {
	v, ok := m[key]
	if !ok {
		return defaultValue
	}
	return v
}
