/**
 * Copyright 2023 KusionStack Authors.
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

package clusterinfo

import (
	"context"
	"strings"
)

const (
	ClusterLabelKey = "kusionstack.io/cluster" // Label key for cluster name that will be attached when use Client or Cache to read

	EnvClusterAllowList = "CLUSTER_ALLOW_LIST" // Comma separated list of cluster names that are allowed to be accessed
	EnvClusterBlockList = "CLUSTER_BLOCK_LIST" // Comma separated list of cluster names that are blocked to be accessed
)

const (
	All      = "all"
	Fed      = "fed"
	Clusters = "clusters"
)

var (
	ContextFed      = WithCluster(context.Background(), Fed)
	ContextClusters = WithCluster(context.Background(), Clusters)
	ContextAll      = WithCluster(context.Background(), All)
)

func WithCluster(parent context.Context, cluster string) context.Context {
	return context.WithValue(parent, ClusterLabelKey, cluster)
}

func GetCluster(ctx context.Context) (string, bool) {
	cluster, ok := ctx.Value(ClusterLabelKey).(string)
	return cluster, ok
}

func WithClusters(parent context.Context, cluster []string) context.Context {
	return WithCluster(parent, strings.Join(cluster, ","))
}

func GetClusters(ctx context.Context) ([]string, bool) {
	clusters, ok := GetCluster(ctx)
	return strings.Split(clusters, ","), ok
}
