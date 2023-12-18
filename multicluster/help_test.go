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
	"encoding/json"
	"fmt"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"kusionstack.io/kube-utils/multicluster/clusterinfo"
)

var (
	podInput = `
	{
		"apiVersion": "v1",
		"kind": "Pod",
		"metadata": {"labels":{"app":"httpbin","pod-template-hash":"59b4c7484c"},"name":"httpbin-59b4c7484c-xwmfw","namespace":"default"},
		"spec": {
			"containers": [
				{
					"image": "kennethreitz/httpbin",
					"imagePullPolicy": "IfNotPresent",
					"name": "httpbin",
					"ports": [
						{
							"containerPort": 80
						}
					]
				}
			],
			"preemptionPolicy": "PreemptLowerPriority",
			"priority": 0,
			"serviceAccountName": "default",
			"terminationGracePeriodSeconds": 0,
			"tolerations": [
				{
					"effect": "NoExecute",
					"key": "node.kubernetes.io/not-ready",
					"operator": "Exists",
					"tolerationSeconds": 300
				},
				{
					"effect": "NoExecute",
					"key": "node.kubernetes.io/unreachable",
					"operator": "Exists",
					"tolerationSeconds": 300
				}
			]
		}
	}`

	podListInput = `
	{
		"apiVersion": "v1",
		"items": [
			{
				"apiVersion": "v1",
				"kind": "Pod",
				"metadata": {"labels":{"app":"httpbin","pod-template-hash":"59b4c7484c"},"name":"httpbin-59b4c7484c-xwmfw","namespace":"default"},
				"spec": {
					"containers": [
						{
							"image": "kennethreitz/httpbin",
							"imagePullPolicy": "IfNotPresent",
							"name": "httpbin",
							"ports": [
								{
									"containerPort": 80
								}
							]
						}
					],
					"preemptionPolicy": "PreemptLowerPriority",
					"priority": 0,
					"serviceAccountName": "default",
					"terminationGracePeriodSeconds": 0,
					"tolerations": [
						{
							"effect": "NoExecute",
							"key": "node.kubernetes.io/not-ready",
							"operator": "Exists",
							"tolerationSeconds": 300
						},
						{
							"effect": "NoExecute",
							"key": "node.kubernetes.io/unreachable",
							"operator": "Exists",
							"tolerationSeconds": 300
						}
					]
				}
			}
		],
		"kind": "List",
		"metadata": {
			"resourceVersion": ""
		}
	}`
)

var _ = Describe("help", func() {
	It("attach cluster to label", func() {
		var podListObj client.ObjectList = &corev1.PodList{}
		err := json.Unmarshal([]byte(podListInput), podListObj)
		Expect(err).NotTo(HaveOccurred())

		err = attachClusterTo(podListObj, "cluster1")
		Expect(err).NotTo(HaveOccurred())

		podList, ok := podListObj.(*corev1.PodList)
		Expect(ok).To(BeTrue())
		for _, pod := range podList.Items {
			cluster, ok := pod.GetLabels()[clusterinfo.ClusterLabelKey]
			Expect(ok).To(BeTrue())
			Expect(cluster).To(Equal("cluster1"))
		}

		var podObj client.Object = &corev1.Pod{}
		err = json.Unmarshal([]byte(podInput), podObj)
		Expect(err).NotTo(HaveOccurred())

		err = attachClusterTo(podObj, "cluster2")
		Expect(err).NotTo(HaveOccurred())

		pod, ok := podObj.(*corev1.Pod)
		Expect(ok).To(BeTrue())
		cluster, ok := pod.GetLabels()[clusterinfo.ClusterLabelKey]
		Expect(ok).To(BeTrue())
		Expect(cluster).To(Equal("cluster2"))
	})

	It("get cluster from context and label", func() {
		cases := []struct {
			context context.Context
			labels  map[string]string
			cluster string
			err     error
		}{
			{
				context: clusterinfo.ContextFed,
				labels:  map[string]string{},
				cluster: clusterinfo.Fed,
			},
			{
				context: clusterinfo.WithCluster(context.Background(), "cluster2"),
				labels:  map[string]string{clusterinfo.ClusterLabelKey: "cluster2"},
				cluster: "cluster2",
				err:     nil,
			},
			{
				context: clusterinfo.ContextFed,
				labels:  map[string]string{clusterinfo.ClusterLabelKey: "cluster2"},
				cluster: "",
				err:     fmt.Errorf("invalid cluster"),
			},
		}
		for _, v := range cases {
			cluster, err := getClusterName(v.context, v.labels)
			if v.err != nil {
				Expect(err).To(HaveOccurred())
			} else {
				Expect(err).NotTo(HaveOccurred())
			}
			Expect(cluster).To(Equal(v.cluster))
		}
	})

	It("check clusters", func() {
		cases := []struct {
			clusters []string
			err      error
		}{
			{
				clusters: []string{"cluster1", "cluster2"},
				err:      nil,
			},
			{
				clusters: []string{"cluster1", "cluster1", "cluster3"},
				err:      fmt.Errorf("invalid clusters"),
			},
			{
				clusters: []string{clusterinfo.Fed, "cluster1", "cluster2"},
				err:      nil,
			},
			{
				clusters: []string{clusterinfo.Fed, clusterinfo.Clusters},
				err:      nil,
			},
			{
				clusters: []string{clusterinfo.Fed, clusterinfo.Fed},
				err:      fmt.Errorf("invalid clusters"),
			},
			{
				clusters: []string{clusterinfo.All, clusterinfo.All},
				err:      fmt.Errorf("invalid clusters"),
			},
			{
				clusters: []string{clusterinfo.All, clusterinfo.Fed},
				err:      fmt.Errorf("invalid clusters"),
			},
			{
				clusters: []string{clusterinfo.All, "cluster1", "cluster2"},
				err:      fmt.Errorf("invalid clusters"),
			},
		}
		for _, v := range cases {
			err := checkClusters(v.clusters)
			if v.err != nil {
				Expect(err).To(HaveOccurred())
			} else {
				Expect(err).NotTo(HaveOccurred())
			}
		}
	})
})
