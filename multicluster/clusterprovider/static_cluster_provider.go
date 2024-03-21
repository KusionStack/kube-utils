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

package clusterprovider

import (
	"context"

	"k8s.io/client-go/rest"
)

// StaticClusterProvider is a controller that manages a static set of clusters.
type StaticClusterProvider struct {
	clusterToConfig  map[string]*rest.Config
	addUpdateHandler func(string, *rest.Config) error
}

func NewStaticClusterProvider(clusterToConfig map[string]*rest.Config) *StaticClusterProvider {
	return &StaticClusterProvider{
		clusterToConfig: clusterToConfig,
	}
}

func (c *StaticClusterProvider) Run(stopCh <-chan struct{}) error {
	if c.addUpdateHandler == nil {
		return nil
	}

	for cluster, config := range c.clusterToConfig {
		if err := c.addUpdateHandler(cluster, config); err != nil {
			return err
		}
	}

	return nil
}

func (c *StaticClusterProvider) AddEventHandler(addUpdateHandler func(string, *rest.Config) error, deleteHandler func(string)) {
	c.addUpdateHandler = addUpdateHandler
}

func (c *StaticClusterProvider) WaitForSynced(ctx context.Context) bool {
	return true
}
