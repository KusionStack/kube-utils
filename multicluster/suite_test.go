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
	"net/url"
	"os"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	"sigs.k8s.io/controller-runtime/pkg/cluster"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	"kusionstack.io/kube-utils/multicluster/clusterinfo"
	"kusionstack.io/kube-utils/multicluster/controller"
)

var (
	fedEnv    *envtest.Environment
	fedClient client.Client

	clusterEnv1    *envtest.Environment
	clusterClient1 client.Client // cluster 1 client

	clusterEnv2    *envtest.Environment
	clusterClient2 client.Client // cluster 2 client

	clusterClient client.Client // multi cluster client
	clusterCache  cache.Cache   // multi cluster cache

	ctx    context.Context
	cancel context.CancelFunc

	manager *Manager
)

var _ = BeforeSuite(func() {
	logf.SetLogger(zap.New(zap.WriteTo(os.Stdout), zap.UseDevMode(true)))

	ctx, cancel = context.WithCancel(context.TODO())
	By("bootstrapping test environment")

	// fed
	fedScheme := runtime.NewScheme()
	err := appsv1.SchemeBuilder.AddToScheme(fedScheme) // deployment
	Expect(err).NotTo(HaveOccurred())

	fedEnv = &envtest.Environment{
		Scheme: fedScheme,
	}
	fedEnv.ControlPlane.GetAPIServer().URL = &url.URL{
		Host: "127.0.0.1:10001",
	}
	fedConfig, err := fedEnv.Start()
	Expect(err).NotTo(HaveOccurred())
	Expect(fedConfig).NotTo(BeNil())

	fedClient, err = client.New(fedConfig, client.Options{Scheme: fedScheme})
	Expect(err).NotTo(HaveOccurred())
	Expect(fedClient).NotTo(BeNil())

	// cluster 1
	clusterScheme := runtime.NewScheme()
	err = corev1.SchemeBuilder.AddToScheme(clusterScheme) // configmap and service
	Expect(err).NotTo(HaveOccurred())
	err = appsv1.SchemeBuilder.AddToScheme(clusterScheme) // deployment
	Expect(err).NotTo(HaveOccurred())

	clusterEnv1 = &envtest.Environment{
		Scheme: clusterScheme,
	}
	clusterEnv1.ControlPlane.GetAPIServer().URL = &url.URL{
		Host: "127.0.0.1:10002",
	}
	clusterConfig1, err := clusterEnv1.Start()
	Expect(err).NotTo(HaveOccurred())
	Expect(clusterConfig1).NotTo(BeNil())

	clusterClient1, err = client.New(clusterConfig1, client.Options{Scheme: clusterScheme})
	Expect(err).NotTo(HaveOccurred())
	Expect(clusterClient1).NotTo(BeNil())

	// cluster 2
	clusterEnv2 = &envtest.Environment{
		Scheme: clusterScheme,
	}
	clusterEnv2.ControlPlane.GetAPIServer().URL = &url.URL{
		Host: "127.0.0.1:10003",
	}
	clusterConfig2, err := clusterEnv2.Start()
	Expect(err).NotTo(HaveOccurred())
	Expect(clusterConfig2).NotTo(BeNil())

	clusterClient2, err = client.New(clusterConfig2, client.Options{Scheme: clusterScheme})
	Expect(err).NotTo(HaveOccurred())
	Expect(clusterClient2).NotTo(BeNil())

	// manager
	var (
		newCacheFunc  cache.NewCacheFunc
		newClientFunc cluster.NewClientFunc
	)
	os.Setenv(clusterinfo.EnvClusterAllowList, "cluster1,cluster2")
	manager, newCacheFunc, newClientFunc, err = NewManager(&ManagerConfig{
		FedConfig:     fedConfig,
		ClusterScheme: clusterScheme,
		ResyncPeriod:  10 * time.Minute,

		RestConfigForCluster: func(clusterName string) *rest.Config {
			switch clusterName {
			case "cluster1":
				return clusterConfig1
			case "cluster2":
				return clusterConfig2
			default:
				return fedConfig
			}
		},
		ClusterManagermentGVR: &schema.GroupVersionResource{
			Group:    "apps",
			Version:  "v1",
			Resource: "deployments",
		},
		ClusterManagermentType: controller.TestCluterManagement,
	}, Options{})
	Expect(err).NotTo(HaveOccurred())
	Expect(manager).NotTo(BeNil())
	Expect(newCacheFunc).NotTo(BeNil())
	Expect(newClientFunc).NotTo(BeNil())

	// multiClusterCache
	mapper, err := apiutil.NewDynamicRESTMapper(fedConfig)
	Expect(err).NotTo(HaveOccurred())

	clusterCache, err = newCacheFunc(fedConfig, cache.Options{
		Scheme: fedScheme,
		Mapper: mapper,
	})
	Expect(err).NotTo(HaveOccurred())
	go clusterCache.Start(ctx)

	// multiClusterClient
	clusterClient, err = newClientFunc(clusterCache, fedConfig, client.Options{
		Scheme: fedScheme,
		Mapper: mapper,
	})
	Expect(err).NotTo(HaveOccurred())
	Expect(clusterClient).NotTo(BeNil())

	go manager.Run(2, ctx)
})

var _ = AfterSuite(func() {
	cancel()
	By("tearing down the test environment")

	err := fedEnv.Stop()
	Expect(err).NotTo(HaveOccurred())

	err = clusterEnv1.Stop()
	Expect(err).NotTo(HaveOccurred())

	err = clusterEnv2.Stop()
	Expect(err).NotTo(HaveOccurred())
})

func TestMultiCluster(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "multicluster suite test")
}
