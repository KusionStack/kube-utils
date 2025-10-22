/**
 * Copyright 2025 KusionStack Authors.
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
	"fmt"
	"maps"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/discovery"
)

// MultiClusterDiscovery provides fed and member clusters discovery interface
type MultiClusterDiscoveryManager interface {
	// GetAllDiscoveryInterface returns the fed and member clusters discovery interface
	GetAllDiscoveryInterface() (fed discovery.DiscoveryInterface, members map[string]discovery.DiscoveryInterface)
}

func (mcc *multiClusterClient) GetAllDiscoveryInterface() (discovery.DiscoveryInterface, map[string]discovery.DiscoveryInterface) {
	mcc.mutex.RLock()
	defer mcc.mutex.RUnlock()
	copy := make(map[string]discovery.DiscoveryInterface)
	maps.Copy(copy, mcc.clusterToDiscoveryClient)
	return mcc.fedDiscovery, copy
}

func GetAllClusterServerGroupsAndResources(membersDiscoveryClients map[string]discovery.DiscoveryInterface) ([]*metav1.APIGroup, []*metav1.APIResourceList, error) {
	totalClusters := len(membersDiscoveryClients)
	if totalClusters == 0 {
		return nil, nil, nil
	}
	// If there is only one cluster, we can use the cached discovery client to get the server groups and resources
	if totalClusters == 1 {
		for _, cachedClient := range membersDiscoveryClients {
			return cachedClient.ServerGroupsAndResources()
		}
	}

	// If there are multiple clusters, we need to get the intersection of groups and resources
	var (
		groupVersionCount     = make(map[string]int)
		groupVersionNameCount = make(map[string]int)

		apiGroupsRes            []*metav1.APIGroup
		apiResourceListsRes     []*metav1.APIResourceList
		groupVersionToResources = make(map[string][]metav1.APIResource)
	)
	for _, discoveryClient := range membersDiscoveryClients {
		apiGroups, apiResourceLists, err := discoveryClient.ServerGroupsAndResources()
		if err != nil {
			return nil, nil, err
		}

		for _, apiGroup := range apiGroups {
			groupVersion := apiGroup.PreferredVersion.GroupVersion

			if _, ok := groupVersionCount[groupVersion]; !ok {
				groupVersionCount[groupVersion] = 1
			} else {
				groupVersionCount[groupVersion]++

				if groupVersionCount[groupVersion] == totalClusters { // all clusters have this PreferredVersion
					apiGroupsRes = append(apiGroupsRes, apiGroup)
				}
			}
		}

		for _, apiResourceList := range apiResourceLists {
			for i := range apiResourceList.APIResources {
				apiResource := apiResourceList.APIResources[i]
				groupVersionName := fmt.Sprintf("%s/%s", apiResourceList.GroupVersion, apiResource.Name)

				if _, ok := groupVersionNameCount[groupVersionName]; !ok {
					groupVersionNameCount[groupVersionName] = 1
				} else {
					groupVersionNameCount[groupVersionName]++

					if groupVersionNameCount[groupVersionName] == totalClusters { // all clusters have this GroupVersion and Name
						groupVersionToResources[apiResourceList.GroupVersion] = append(groupVersionToResources[apiResourceList.GroupVersion], apiResource)
					}
				}
			}
		}
	}

	for groupVersion, resources := range groupVersionToResources {
		apiResourceList := metav1.APIResourceList{
			TypeMeta:     metav1.TypeMeta{Kind: "APIResourceList", APIVersion: "v1"},
			GroupVersion: groupVersion,
		}
		apiResourceList.APIResources = append(apiResourceList.APIResources, resources...)
		apiResourceListsRes = append(apiResourceListsRes, &apiResourceList)
	}

	return apiGroupsRes, apiResourceListsRes, nil
}
