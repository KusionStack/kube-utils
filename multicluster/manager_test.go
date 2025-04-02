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
	"os"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	utilcache "kusionstack.io/kube-utils/cache"
	"kusionstack.io/kube-utils/multicluster/clusterinfo"
)

var _ = Describe("multicluster", func() {
	It("fed adds 4 clusters", func() {
		for i := 1; i < 5; i++ {
			name := fmt.Sprintf("cluster%d", i)

			var replicas int32 = 1
			err := fedClient.Create(ctx, &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      name,
				},
				Spec: appsv1.DeploymentSpec{
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{
							"cluster": name,
						},
					},
					Replicas: &replicas,
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								"cluster": name,
							},
						},
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name:  name,
									Image: "kennethreitz/httpbin",
								},
							},
						},
					},
				},
			})
			Expect(err).NotTo(HaveOccurred())
		}

		var deployments appsv1.DeploymentList
		err := fedClient.List(ctx, &deployments)
		Expect(err).NotTo(HaveOccurred())
		Expect(deployments.Items).To(HaveLen(4))
	})

	It("cluster1 creates 2 services", func() {
		for i := range 2 {
			name := fmt.Sprintf("cluster1-%d", i)

			err := clusterClient1.Create(ctx, &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      name,
					Labels: map[string]string{
						"cluster": "cluster1",
					},
				},
				Spec: corev1.ServiceSpec{
					Ports: []corev1.ServicePort{
						{
							Name: "https",
							Port: 443,
						},
					},
					Type: corev1.ServiceTypeClusterIP,
				},
			})
			Expect(err).NotTo(HaveOccurred())
		}

		var services corev1.ServiceList
		err := clusterClient1.List(ctx, &services, client.MatchingLabels{
			"cluster": "cluster1",
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(services.Items).To(HaveLen(2))
	})

	It("cluster1 creates 2 configmaps", func() {
		for i := range 2 {
			name := fmt.Sprintf("cluster1-%d", i)

			err := clusterClient1.Create(ctx, &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      name,
					Labels: map[string]string{
						"cluster": "cluster1",
					},
				},
				Data: map[string]string{
					"a": "1",
					"b": "2",
				},
			})
			Expect(err).NotTo(HaveOccurred())
		}

		var configmaps corev1.ConfigMapList
		err := clusterClient1.List(ctx, &configmaps, client.MatchingLabels{
			"cluster": "cluster1",
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(configmaps.Items).To(HaveLen(2))
	})

	It("cluster2 creates 3 configmaps", func() {
		for i := range 3 {
			name := fmt.Sprintf("cluster2-%d", i)

			err := clusterClient2.Create(ctx, &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      name,
					Labels: map[string]string{
						"cluster": "cluster2",
					},
				},
				Data: map[string]string{
					"a": "1",
					"b": "2",
				},
			})
			Expect(err).NotTo(HaveOccurred())
		}

		var configmaps corev1.ConfigMapList
		err := clusterClient2.List(ctx, &configmaps, client.MatchingLabels{
			"cluster": "cluster2",
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(configmaps.Items).To(HaveLen(3))
	})

	It("fed has synced clusters", func() {
		manager.WaitForSynced(ctx)
		clusters := manager.SyncedClusters()
		Expect(clusters).To(HaveLen(2))
		Expect(clusters).To(ContainElements([]string{"cluster1", "cluster2"})) // ignored cluster3 cluster4
	})

	It("multiClusterCache has synced", func() {
		synced := clusterCache.WaitForCacheSync(clusterinfo.ContextAll)
		Expect(synced).To(BeTrue())
	})

	It("multiClusterClient get server groups and resources", func() {
		mcdiscovery, ok := clusterClient.(MultiClusterDiscovery)
		Expect(ok).To(BeTrue())
		cachedDiscoveryClient := mcdiscovery.MembersCachedDiscoveryInterface()
		apiGroups, apiResourceLists, err := cachedDiscoveryClient.ServerGroupsAndResources()
		Expect(err).NotTo(HaveOccurred())

		groupVersionSets := sets.NewString()
		for _, apiGroup := range apiGroups {
			groupVersion := apiGroup.PreferredVersion.GroupVersion
			groupVersionSets.Insert(groupVersion)
		}
		Expect(groupVersionSets.HasAll("apps/v1", "v1")).To(BeTrue())

		apiResourceSets := sets.NewString()
		for _, apiResourceList := range apiResourceLists {
			for _, apiResource := range apiResourceList.APIResources {
				groupVersionName := fmt.Sprintf("%s/%s", apiResourceList.GroupVersion, apiResource.Name)
				apiResourceSets.Insert(groupVersionName)
			}
		}
		Expect(apiResourceSets.HasAll("apps/v1/deployments", "apps/v1/deployments/status", "v1/configmaps")).To(BeTrue())
	})

	It("multiClusterClient update the deployment status of cluster1", func() {
		var deployment appsv1.Deployment
		err := clusterClient.Get(clusterinfo.ContextFed, client.ObjectKey{
			Namespace: "default",
			Name:      "cluster1",
		}, &deployment)
		Expect(err).NotTo(HaveOccurred())

		deployment.Status.ObservedGeneration = 100
		err = clusterClient.Status().Update(clusterinfo.ContextFed, &deployment)
		Expect(err).NotTo(HaveOccurred())

		time.Sleep(300 * time.Millisecond)

		var deployment1 appsv1.Deployment
		err = clusterClient.Get(clusterinfo.ContextFed, client.ObjectKey{
			Namespace: "default",
			Name:      "cluster1",
		}, &deployment1)
		Expect(err).NotTo(HaveOccurred())
		Expect(deployment1.Status.ObservedGeneration).To(Equal(int64(100)))
	})

	It("multiClusterClient list the services of cluster1", func() {
		clusterCtx := clusterinfo.WithClusters(ctx, []string{"cluster1"})

		var services corev1.ServiceList
		err := clusterClient.List(clusterCtx, &services, client.MatchingLabels{
			"cluster": "cluster1",
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(services.Items).To(HaveLen(2))

		for _, item := range services.Items {
			cluster, ok := item.ObjectMeta.Labels[clusterinfo.ClusterLabelKey]
			Expect(ok).To(BeTrue())
			Expect(cluster).To(Equal("cluster1"))
		}
	})

	It("multiClusterClient list the configmaps of cluster1 and cluster2", func() {
		clusterCtx := clusterinfo.WithClusters(ctx, []string{"cluster1"})

		var configmaps corev1.ConfigMapList
		err := clusterClient.List(clusterCtx, &configmaps, &client.ListOptions{
			Namespace: "default",
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(configmaps.Items).To(HaveLen(2))

		for _, item := range configmaps.Items {
			cluster, ok := item.ObjectMeta.Labels[clusterinfo.ClusterLabelKey]
			Expect(ok).To(BeTrue())
			Expect(cluster).To(Equal("cluster1"))
		}

		configmaps = corev1.ConfigMapList{}
		clusterCtx = clusterinfo.WithClusters(ctx, []string{"cluster2"})
		err = clusterClient.List(clusterCtx, &configmaps, &client.ListOptions{
			Namespace: "default",
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(configmaps.Items).To(HaveLen(3))

		for _, item := range configmaps.Items {
			cluster, ok := item.ObjectMeta.Labels[clusterinfo.ClusterLabelKey]
			Expect(ok).To(BeTrue())
			Expect(cluster).To(Equal("cluster2"))
		}

		configmaps = corev1.ConfigMapList{}
		clusterCtx = clusterinfo.WithClusters(ctx, []string{"cluster1", "cluster2"})
		err = clusterClient.List(clusterCtx, &configmaps, &client.ListOptions{
			Namespace: "default",
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(configmaps.Items).To(HaveLen(5))

		for _, item := range configmaps.Items {
			cluster, ok := item.ObjectMeta.Labels[clusterinfo.ClusterLabelKey]
			Expect(ok).To(BeTrue())
			Expect(cluster).To(HavePrefix("cluster"))
		}

		configmaps = corev1.ConfigMapList{}
		clusterCtx = clusterinfo.WithCluster(ctx, clusterinfo.Clusters)
		err = clusterClient.List(clusterCtx, &configmaps, &client.ListOptions{
			Namespace: "default",
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(configmaps.Items).To(HaveLen(5))

		for _, item := range configmaps.Items {
			cluster, ok := item.ObjectMeta.Labels[clusterinfo.ClusterLabelKey]
			Expect(ok).To(BeTrue())
			Expect(cluster).To(HavePrefix("cluster"))
		}
	})

	It("multiClusterClient delete then get 1 configmap of cluster2", func() {
		clusterCtx := clusterinfo.WithCluster(ctx, "cluster2")

		configmap := corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "default",
				Name:      "cluster2-0",
			},
		}
		err := clusterClient.Get(clusterCtx, client.ObjectKey{
			Namespace: "default",
			Name:      "cluster2-0",
		}, &configmap)
		Expect(err).NotTo(HaveOccurred())
		cluster, ok := configmap.ObjectMeta.Labels[clusterinfo.ClusterLabelKey]
		Expect(ok).To(BeTrue())
		Expect(cluster).To(Equal("cluster2"))

		err = clusterClient.Delete(clusterCtx, &configmap)
		Expect(err).NotTo(HaveOccurred())

		// Label should be preserved after deleting
		val, ok := configmap.GetLabels()[clusterinfo.ClusterLabelKey]
		Expect(ok).To(BeTrue())
		Expect(val).To(Equal("cluster2"))

		configmap.SetLabels(map[string]string{"foo": "bar"})
		err = clusterClient.Update(clusterCtx, &configmap)
		Expect(err).To(HaveOccurred())

		// Label should be preserved after updating
		val, ok = configmap.GetLabels()[clusterinfo.ClusterLabelKey]
		Expect(ok).To(BeTrue())
		Expect(val).To(Equal("cluster2"))

		time.Sleep(300 * time.Millisecond)

		err = clusterClient.Get(clusterCtx, client.ObjectKey{
			Namespace: "default",
			Name:      "cluster2-0",
		}, &configmap)
		Expect(err).To(HaveOccurred())
	})

	It("multiClusterClient delete all other configmaps of cluster2", func() {
		clusterCtx := clusterinfo.WithCluster(ctx, "cluster2")

		var configmaps corev1.ConfigMapList
		err := clusterClient.List(clusterCtx, &configmaps, &client.ListOptions{
			Namespace: "default",
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(configmaps.Items).To(HaveLen(2))

		var configmap corev1.ConfigMap
		err = clusterClient.DeleteAllOf(clusterCtx, &configmap, &client.DeleteAllOfOptions{
			ListOptions: client.ListOptions{
				Namespace: "default",
			},
		})
		Expect(err).NotTo(HaveOccurred())

		time.Sleep(300 * time.Millisecond)

		configmaps = corev1.ConfigMapList{}
		err = clusterClient.List(clusterCtx, &configmaps, &client.ListOptions{
			Namespace: "default",
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(configmaps.Items).To(BeEmpty())
	})

	It("multiClusterClient patch the configmap of cluster1", func() {
		clusterCtx := clusterinfo.WithCluster(ctx, "cluster1")

		var configmap corev1.ConfigMap
		err := clusterClient.Get(clusterCtx, client.ObjectKey{
			Namespace: "default",
			Name:      "cluster1-0",
		}, &configmap)
		Expect(err).NotTo(HaveOccurred())

		patch := []byte(`{"metadata":{"labels":{"patch": "cluster1"}}}`)
		err = clusterClient.Patch(clusterCtx, &configmap, client.RawPatch(types.StrategicMergePatchType, patch))
		Expect(err).NotTo(HaveOccurred())

		time.Sleep(1 * time.Second)

		var configmap1 corev1.ConfigMap
		err = clusterClient.Get(clusterCtx, client.ObjectKey{
			Namespace: "default",
			Name:      "cluster1-0",
		}, &configmap1)
		Expect(err).NotTo(HaveOccurred())
		Expect(configmap1.ObjectMeta.Labels["patch"]).To(Equal("cluster1"))
	})

	It("get configmap informer, and then check whether it has synced", func() {
		var configmap corev1.ConfigMap
		clusterCtx := clusterinfo.WithClusters(ctx, []string{"cluster1", "cluster2"})

		informer, err := clusterCache.GetInformer(clusterCtx, &configmap)
		Expect(err).NotTo(HaveOccurred())
		Expect(informer.HasSynced()).To(BeTrue())

		clusterCtx = clusterinfo.WithClusters(ctx, []string{clusterinfo.Clusters})

		informer, err = clusterCache.GetInformer(clusterCtx, &configmap)
		Expect(err).NotTo(HaveOccurred())
		Expect(informer.HasSynced()).To(BeTrue())
	})

	It("remove cluster2, then list the services of cluster2", func() {
		err := fedClient.Delete(ctx, &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "default",
				Name:      "cluster2",
			},
		})
		Expect(err).NotTo(HaveOccurred())

		time.Sleep(300 * time.Millisecond)

		var deployments appsv1.DeploymentList
		err = fedClient.List(ctx, &deployments)
		Expect(err).NotTo(HaveOccurred())
		Expect(deployments.Items).To(HaveLen(3)) // cluster1 cluster4 cluster5
		Expect(manager.SyncedClusters()).To(Equal([]string{"cluster1"}))

		clusterCtx := clusterinfo.WithClusters(ctx, []string{"cluster2"})
		var services corev1.ServiceList
		err = clusterClient.List(clusterCtx, &services, client.MatchingLabels{
			"cluster": "cluster2",
		})
		Expect(err).To(HaveOccurred())
	})

	It("get cluster2 configmap informer", func() {
		var configmap corev1.ConfigMap
		clusterCtx := clusterinfo.WithClusters(ctx, []string{"cluster2"})

		_, err := clusterCache.GetInformer(clusterCtx, &configmap)
		Expect(err).To(HaveOccurred())
	})

	It("MultiClusterClient supports DisableDeepCopy options", func() {
		mcc := clusterClient.(*multiClusterClient)
		mockClient := newMockClient()
		mcc.clusterToClient["mock"] = mockClient
		mcc.List(clusterinfo.WithClusters(ctx, []string{"mock"}), &corev1.PodList{}, utilcache.DisableDeepCopy)
		Expect(mockClient.Observed).Should(BeTrue())
	})

	It("MultiClusterCache supports DisableDeepCopy options", func() {
		mcc := clusterCache.(*multiClusterCache)
		mockCache := &MockCache{Cache: nil}
		mcc.clusterToCache["mock"] = mockCache
		mcc.List(clusterinfo.WithClusters(ctx, []string{"mock"}), &corev1.PodList{}, utilcache.DisableDeepCopy)
		Expect(mockCache.Observed).Should(BeTrue())
	})
})

var _ = Describe("test cluster filter", func() {
	AfterEach(func() {
		os.Unsetenv(clusterinfo.EnvClusterAllowList)
		os.Unsetenv(clusterinfo.EnvClusterBlockList)
	})

	It("both allow and block lists are used", func() {
		err := os.Setenv(clusterinfo.EnvClusterAllowList, "cluster1,cluster2")
		Expect(err).NotTo(HaveOccurred())

		err = os.Setenv(clusterinfo.EnvClusterBlockList, "cluster3,cluster4")
		Expect(err).NotTo(HaveOccurred())

		clusterFilter, err := getClusterFilter(&ManagerConfig{})
		Expect(err).To(HaveOccurred())
		Expect(clusterFilter).To(BeNil())
	})

	It("ClusterFilter and allow lists are used", func() {
		err := os.Setenv(clusterinfo.EnvClusterAllowList, "cluster1,cluster2")
		Expect(err).NotTo(HaveOccurred())

		clusterFilter, err := getClusterFilter(&ManagerConfig{
			ClusterFilter: func(cluster string) bool {
				return cluster == "cluster1"
			},
		})
		Expect(err).To(HaveOccurred())
		Expect(clusterFilter).To(BeNil())
	})

	It("ClusterFilter and block lists are used", func() {
		err := os.Setenv(clusterinfo.EnvClusterBlockList, "cluster1,cluster2")
		Expect(err).NotTo(HaveOccurred())

		clusterFilter, err := getClusterFilter(&ManagerConfig{
			ClusterFilter: func(cluster string) bool {
				return cluster == "cluster3"
			},
		})
		Expect(err).To(HaveOccurred())
		Expect(clusterFilter).To(BeNil())
	})

	It("use allow lists", func() {
		err := os.Setenv(clusterinfo.EnvClusterAllowList, "cluster1,cluster2")
		Expect(err).NotTo(HaveOccurred())

		clusterFilter, err := getClusterFilter(&ManagerConfig{})
		Expect(err).NotTo(HaveOccurred())
		Expect(clusterFilter("cluster1")).To(BeTrue())
		Expect(clusterFilter("cluster2")).To(BeTrue())
	})

	It("use block lists", func() {
		err := os.Setenv(clusterinfo.EnvClusterBlockList, "cluster1,cluster2")
		Expect(err).NotTo(HaveOccurred())

		clusterFilter, err := getClusterFilter(&ManagerConfig{})
		Expect(err).NotTo(HaveOccurred())
		Expect(clusterFilter("cluster1")).To(BeFalse())
		Expect(clusterFilter("cluster2")).To(BeFalse())
	})

	It("use ClusterFilter", func() {
		clusterFilter, err := getClusterFilter(&ManagerConfig{
			ClusterFilter: func(cluster string) bool {
				if cluster == "cluster1" {
					return true
				}
				if cluster == "cluster2" {
					return true
				}
				return false
			},
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(clusterFilter("cluster1")).To(BeTrue())
		Expect(clusterFilter("cluster3")).To(BeFalse())
	})
})

func newMockClient() *MockClient {
	clientBuilder := fake.NewClientBuilder()
	return &MockClient{
		WithWatch: clientBuilder.Build(),
	}
}

type MockClient struct {
	client.WithWatch

	Observed bool
}

func (mc *MockClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	for _, opt := range opts {
		if opt == utilcache.DisableDeepCopy {
			mc.Observed = true
		}
	}

	return nil
}

type MockCache struct {
	cache.Cache

	Observed bool
}

func (mc *MockCache) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	for _, opt := range opts {
		if opt == utilcache.DisableDeepCopy {
			mc.Observed = true
		}
	}

	return nil
}
