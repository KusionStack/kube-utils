/**
 * Copyright 2023 The KusionStack Authors
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

package client

import (
	"fmt"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

func ObjectKeyString(obj client.Object) string {
	if len(obj.GetNamespace()) == 0 {
		return obj.GetName()
	}
	return fmt.Sprintf("%s/%s", obj.GetNamespace(), obj.GetName())
}

func AdmissionRequestObjectKeyString(req admission.Request) string {
	if len(req.Namespace) == 0 {
		return req.Name
	}
	return fmt.Sprintf("%s/%s", req.Namespace, req.Name)
}
