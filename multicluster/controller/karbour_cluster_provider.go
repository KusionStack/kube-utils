/**
 * Copyright 2024 KusionStack Authors.
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

package controller

import (
	"fmt"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"

	clusterv1beta1 "kusionstack.io/kube-api/cluster/v1beta1"
)

var _ ClusterProvider = &KarbourClusterProvider{}

type KarbourClusterProvider struct {
	config *rest.Config
}

func (p *KarbourClusterProvider) Init(config *rest.Config) {
	p.config = config
}

func (p *KarbourClusterProvider) GetClusterMangementGVR() schema.GroupVersionResource {
	return clusterv1beta1.SchemeGroupVersion.WithResource("clusters")
}

func (p *KarbourClusterProvider) GetClusterName(obj *unstructured.Unstructured) string {
	if obj == nil {
		return ""
	}
	return obj.GetName()
}

func (p *KarbourClusterProvider) GetClusterConfig(obj *unstructured.Unstructured) *rest.Config {
	clusterName := p.GetClusterName(obj)
	if clusterName == "" || p.config == nil {
		return nil
	}

	gvr := p.GetClusterMangementGVR()

	clusterConfig := *p.config
	clusterConfig.Host = fmt.Sprintf("%s/apis/%s/%s/%s/%s/proxy", clusterConfig.Host, gvr.Group, gvr.Version, gvr.Resource, clusterName)

	return &clusterConfig
}
