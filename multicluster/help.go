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

package multicluster

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"kusionstack.io/kube-utils/multicluster/clusterinfo"
)

func attachClusterToObjects(cluster string, objects ...runtime.Object) {
	for _, obj := range objects {
		metaOjbect := obj.(metav1.Object)
		labels := metaOjbect.GetLabels()
		if labels == nil {
			labels = make(map[string]string)
		}
		labels[clusterinfo.ClusterLabelKey] = cluster
		metaOjbect.SetLabels(labels)
	}
}

func getCluster(ctx context.Context, label map[string]string) (cluster string, err error) {
	clusterFromContext, ok1 := clusterinfo.GetCluster(ctx)
	if ok1 {
		cluster = clusterFromContext
	}

	clusterFromLabel, ok2 := label[clusterinfo.ClusterLabelKey]
	if ok2 {
		cluster = clusterFromLabel
	}

	if (ok1 && ok2 && clusterFromContext != clusterFromLabel) || (!ok1 && !ok2) {
		return "", fmt.Errorf("invalid cluster")
	}
	return
}

func getThenDeleteCluster(ctx context.Context, obj client.Object) (cluster string, err error) {
	labels := obj.GetLabels()
	cluster, err = getCluster(ctx, labels)
	if err != nil {
		return
	}
	delete(labels, clusterinfo.ClusterLabelKey)
	obj.SetLabels(labels)
	return
}

func checkClusters(clusters []string) error {
	var fedCount, allCount, clustersCount, clusterCount int
	hasCluster := map[string]struct{}{}

	for _, cluster := range clusters {
		switch cluster {
		case clusterinfo.All:
			allCount++
		case clusterinfo.Clusters:
			clustersCount++
		case clusterinfo.Fed:
			fedCount++
		default:
			if _, ok := hasCluster[cluster]; ok {
				return fmt.Errorf("invalid clusters")
			}
			hasCluster[cluster] = struct{}{}
			clusterCount++
		}
	}

	if fedCount+allCount > 1 ||
		(allCount > 0 && clustersCount > 1) ||
		(allCount > 0 && clusterCount > 1) {
		return fmt.Errorf("invalid clusters")
	}
	return nil
}
