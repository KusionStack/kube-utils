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
	"k8s.io/apiserver/pkg/endpoints/handlers/fieldmanager"
	"k8s.io/client-go/discovery"
	"k8s.io/kube-openapi/pkg/util/proto"
)

func GetOpenAPIModels(discoveryClient discovery.OpenAPISchemaInterface) (proto.Models, error) {
	doc, err := discoveryClient.OpenAPISchema()
	if err != nil {
		return nil, err
	}
	return proto.NewOpenAPIData(doc)
}

func NewTypeConverterByDiscovery(discoveryClient discovery.DiscoveryInterface) (fieldmanager.TypeConverter, error) {
	models, err := GetOpenAPIModels(discoveryClient)
	if err != nil {
		return nil, err
	}
	return fieldmanager.NewTypeConverter(models, true)
}
