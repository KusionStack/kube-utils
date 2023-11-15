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
	"fmt"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"kusionstack.io/kube-utils/multicluster/clusterinfo"
)

var _ = Describe("multicluster with 1 fed and 4 clusters", func() {
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
		Expect(len(deployments.Items)).To(Equal(4))
	})

	It("cluster1 creates 2 services", func() {
		for i := 0; i < 2; i++ {
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
		Expect(len(services.Items)).To(Equal(2))
	})

	It("cluster1 creates 2 configmaps", func() {
		for i := 0; i < 2; i++ {
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
		Expect(len(configmaps.Items)).To(Equal(2))
	})

	It("cluster2 creates 3 configmaps", func() {
		for i := 0; i < 3; i++ {
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
		Expect(len(configmaps.Items)).To(Equal(3))
	})

	It("fed has synced clusters", func() {
		manager.WaitForSynced(ctx)
		clusters := manager.SyncedClusters()
		Expect(len(clusters)).To(Equal(2))
		Expect(clusters).To(ContainElements([]string{"cluster1", "cluster2"})) // ignored cluster3 cluster4
	})

	It("multiClusterCache has synced", func() {
		synced := clusterCache.WaitForCacheSync(clusterinfo.ContextAll)
		Expect(synced).To(Equal(true))
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
		Expect(len(services.Items)).To(Equal(2))

		for _, item := range services.Items {
			cluster, ok := item.ObjectMeta.Labels[clusterinfo.ClusterLabelKey]
			Expect(ok).To(Equal(true))
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
		Expect(len(configmaps.Items)).To(Equal(2))

		for _, item := range configmaps.Items {
			cluster, ok := item.ObjectMeta.Labels[clusterinfo.ClusterLabelKey]
			Expect(ok).To(Equal(true))
			Expect(cluster).To(Equal("cluster1"))
		}

		configmaps = corev1.ConfigMapList{}
		clusterCtx = clusterinfo.WithClusters(ctx, []string{"cluster2"})
		err = clusterClient.List(clusterCtx, &configmaps, &client.ListOptions{
			Namespace: "default",
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(len(configmaps.Items)).To(Equal(3))

		for _, item := range configmaps.Items {
			cluster, ok := item.ObjectMeta.Labels[clusterinfo.ClusterLabelKey]
			Expect(ok).To(Equal(true))
			Expect(cluster).To(Equal("cluster2"))
		}

		configmaps = corev1.ConfigMapList{}
		clusterCtx = clusterinfo.WithClusters(ctx, []string{"cluster1", "cluster2"})
		err = clusterClient.List(clusterCtx, &configmaps, &client.ListOptions{
			Namespace: "default",
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(len(configmaps.Items)).To(Equal(5))

		for _, item := range configmaps.Items {
			cluster, ok := item.ObjectMeta.Labels[clusterinfo.ClusterLabelKey]
			Expect(ok).To(Equal(true))
			Expect(cluster).To(HavePrefix("cluster"))
		}

		configmaps = corev1.ConfigMapList{}
		clusterCtx = clusterinfo.WithCluster(ctx, clusterinfo.Clusters)
		err = clusterClient.List(clusterCtx, &configmaps, &client.ListOptions{
			Namespace: "default",
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(len(configmaps.Items)).To(Equal(5))

		for _, item := range configmaps.Items {
			cluster, ok := item.ObjectMeta.Labels[clusterinfo.ClusterLabelKey]
			Expect(ok).To(Equal(true))
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
		Expect(ok).To(Equal(true))
		Expect(cluster).To(Equal("cluster2"))

		err = clusterClient.Delete(clusterCtx, &configmap)
		Expect(err).NotTo(HaveOccurred())

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
		Expect(len(configmaps.Items)).To(Equal(2))

		var configmap corev1.ConfigMap
		err = clusterClient.DeleteAllOf(clusterCtx, &configmap, &client.DeleteAllOfOptions{
			ListOptions: client.ListOptions{
				Namespace: "default",
			},
		})

		time.Sleep(300 * time.Millisecond)

		configmaps = corev1.ConfigMapList{}
		err = clusterClient.List(clusterCtx, &configmaps, &client.ListOptions{
			Namespace: "default",
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(len(configmaps.Items)).To(Equal(0))
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
		Expect(informer.HasSynced()).To(Equal(true))

		clusterCtx = clusterinfo.WithClusters(ctx, []string{clusterinfo.Clusters})

		informer, err = clusterCache.GetInformer(clusterCtx, &configmap)
		Expect(err).NotTo(HaveOccurred())
		Expect(informer.HasSynced()).To(Equal(true))
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
		Expect(len(deployments.Items)).To(Equal(3)) // cluster1 cluster4 cluster5
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
})
