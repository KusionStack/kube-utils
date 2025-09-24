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

package resourcetopo

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/klog/v2"
)

const defaultTimeoutSecond = 1

func TestResourceTopo(t *testing.T) {
	SetDefaultEventuallyTimeout(defaultTimeoutSecond * time.Second)
	RegisterFailHandler(Fail)
	RunSpecs(t, "resourcetopo suite test")
}

var _ = Describe("test suite with ists config(label selector and virtual rersource)", func() {
	var manager Manager
	var fakeClient *fake.Clientset
	var podHandler, stsHandler, istsHandler *objecthandler
	var podStsRelation, stsIstsRelation *relationHandler

	var podStorage, stsStorage, istsStorage TopoNodeStorage
	var ctx context.Context
	var cancel func()

	checkAll := func() bool {
		return podHandler.matchExpected() &&
			stsHandler.matchExpected() &&
			istsHandler.matchExpected() &&
			podStsRelation.matchExpected() &&
			stsIstsRelation.matchExpected()
	}

	BeforeEach(func() {
		fakeClient = fake.NewSimpleClientset()
		k8sInformerFactory := informers.NewSharedInformerFactory(fakeClient, 0)
		var err error
		manager, err = NewResourcesTopoManager(*buildManagerConfig(buildInspectTopoConfig(k8sInformerFactory)))
		Expect(err).NotTo(HaveOccurred())
		stsStorage, _ = manager.GetTopoNodeStorage(StatefulSetMeta)
		podStorage, _ = manager.GetTopoNodeStorage(PodMeta)
		istsStorage, _ = manager.GetTopoNodeStorage(InspectStatefulSetMeta)
		Expect(stsStorage).NotTo(BeNil())
		Expect(podStorage).NotTo(BeNil())
		Expect(istsStorage).NotTo(BeNil())

		ctx, cancel = context.WithCancel(context.Background())
		podHandler = &objecthandler{}
		Expect(manager.AddNodeHandler(PodMeta, podHandler)).Should(Succeed())
		stsHandler = &objecthandler{}
		Expect(manager.AddNodeHandler(StatefulSetMeta, stsHandler)).Should(Succeed())
		istsHandler = &objecthandler{}
		Expect(manager.AddNodeHandler(InspectStatefulSetMeta, istsHandler)).Should(Succeed())

		podStsRelation = &relationHandler{}
		Expect(manager.AddRelationHandler(StatefulSetMeta, PodMeta, podStsRelation)).Should(Succeed())
		stsIstsRelation = &relationHandler{}
		Expect(manager.AddRelationHandler(InspectStatefulSetMeta, StatefulSetMeta, stsIstsRelation)).Should(Succeed())

		manager.Start(ctx.Done())
		k8sInformerFactory.Start(ctx.Done())
		k8sInformerFactory.WaitForCacheSync(ctx.Done())
	})
	AfterEach(func() {
		if !checkAll() {
			klog.Infof("end with object [sts %s, pod %s, ists %s]", stsHandler.string(), podHandler.string(), istsHandler.string())
			klog.Infof("end with relation [stsIsts %s, podSts %s]", stsIstsRelation.string(), podStsRelation.string())
		}
		cancel()
	})

	It("create single pod", func() {
		podName := "test1"
		podHandler.addCallExpected()
		_, err := fakeClient.CoreV1().Pods(namespaceDefault).Create(ctx, newPod(namespaceDefault, podName), metav1.CreateOptions{})
		Expect(err).ToNot(HaveOccurred())

		Eventually(func(g Gomega) {
			pod, _ := podStorage.GetNode(types.NamespacedName{Namespace: namespaceDefault, Name: podName})
			g.Expect(pod).NotTo(BeNil())
			g.Expect(pod.GetPostOrders()).To(BeEmpty())
			g.Expect(pod.GetPreOrders()).To(BeEmpty())

			g.Expect(pod.TypeInfo()).To(Equal(PodMeta))
			g.Expect(pod.NodeInfo()).To(Equal(types.NamespacedName{Namespace: namespaceDefault, Name: podName}))

			g.Expect(checkAll()).To(BeTrue())
		}).Should(Succeed())
	})

	It("create single statefulSet", func() {
		stsName := "testSts"
		var err error

		stsHandler.addCallExpected()
		istsHandler.addCallExpected()
		istsHandler.relatedCallExpected()
		stsIstsRelation.addCallExpected()
		_, err = fakeClient.AppsV1().StatefulSets(namespaceDefault).Create(ctx, newStatefulSet(namespaceDefault, stsName, "apps", stsName), metav1.CreateOptions{})
		Expect(err).ToNot(HaveOccurred())

		Eventually(func(g Gomega) {
			sts, _ := stsStorage.GetNode(types.NamespacedName{Namespace: namespaceDefault, Name: stsName})
			g.Expect(sts).NotTo(BeNil())

			preOrders := sts.GetPreOrders()
			g.Expect(preOrders).To(HaveLen(1))
			postOrders := sts.GetPostOrders()
			g.Expect(postOrders).To(BeEmpty())
			g.Expect(checkAll()).To(BeTrue())
		}).Should(Succeed())
	})

	It("create pod and sts", func() {
		podName := "testpod"
		stsName := "testSts"

		podHandler.addCallExpected()
		_, err := fakeClient.CoreV1().Pods(namespaceDefault).Create(ctx, newPod(namespaceDefault, podName, "apps", stsName), metav1.CreateOptions{})
		Expect(err).ToNot(HaveOccurred())
		syncStatus(checkAll)

		stsHandler.addCallExpected()
		istsHandler.addCallExpected()
		istsHandler.relatedCallExpected()
		podStsRelation.addCallExpected()
		stsIstsRelation.addCallExpected()
		_, err = fakeClient.AppsV1().StatefulSets(namespaceDefault).Create(ctx, newStatefulSet(namespaceDefault, stsName, "apps", stsName), metav1.CreateOptions{})
		Expect(err).ToNot(HaveOccurred())

		Eventually(func(g Gomega) {
			pod, _ := podStorage.GetNode(types.NamespacedName{Namespace: namespaceDefault, Name: podName})
			g.Expect(pod).NotTo(BeNil())
			g.Expect(pod.GetPostOrders()).To(BeEmpty())
			preOrders := pod.GetPreOrders()
			g.Expect(preOrders).To(HaveLen(1))
			g.Expect(pod.TypeInfo()).To(Equal(PodMeta))
			g.Expect(pod.NodeInfo()).To(Equal(types.NamespacedName{Namespace: namespaceDefault, Name: podName}))
			g.Expect(checkAll()).To(BeTrue())
		}).Should(Succeed())
	})

	It("create sts and pod", func() {
		podName := "testpod2"
		stsName := "testSts2"

		stsHandler.addCallExpected()
		istsHandler.addCallExpected()
		istsHandler.relatedCallExpected()
		stsIstsRelation.addCallExpected()
		_, err := fakeClient.AppsV1().StatefulSets(namespaceDefault).Create(ctx, newStatefulSet(namespaceDefault, stsName, "apps", stsName), metav1.CreateOptions{})
		Expect(err).ToNot(HaveOccurred())
		syncStatus(checkAll)

		podHandler.addCallExpected()
		stsHandler.relatedCallExpected()
		istsHandler.relatedCallExpected()
		podStsRelation.addCallExpected()
		_, err = fakeClient.CoreV1().Pods(namespaceDefault).Create(ctx, newPod(namespaceDefault, podName, "apps", stsName), metav1.CreateOptions{})
		Expect(err).ToNot(HaveOccurred())

		Eventually(func(g Gomega) {
			pod, _ := podStorage.GetNode(types.NamespacedName{Namespace: namespaceDefault, Name: podName})
			g.Expect(pod).NotTo(BeNil())
			g.Expect(pod.GetPostOrders()).To(BeEmpty())
			g.Expect(pod.GetPreOrders()).To(HaveLen(1))
			g.Expect(pod.TypeInfo()).To(Equal(PodMeta))
			g.Expect(pod.NodeInfo()).To(Equal(types.NamespacedName{Namespace: namespaceDefault, Name: podName}))
			g.Expect(checkAll()).To(BeTrue())
		}).Should(Succeed())
	})

	It("create pod1, sts and pod2", func() {
		var err error
		pod1Name := "testpod1"
		pod2Name := "testpos2"
		stsName := "testSts2"

		podHandler.addCallExpected()
		_, err = fakeClient.CoreV1().Pods(namespaceDefault).Create(ctx, newPod(namespaceDefault, pod1Name, "apps", stsName), metav1.CreateOptions{})
		Expect(err).ToNot(HaveOccurred())
		syncStatus(checkAll)

		stsHandler.addCallExpected()
		istsHandler.addCallExpected()
		istsHandler.relatedCallExpected()
		podStsRelation.addCallExpected()
		stsIstsRelation.addCallExpected()
		_, err = fakeClient.AppsV1().StatefulSets(namespaceDefault).Create(ctx, newStatefulSet(namespaceDefault, stsName, "apps", stsName), metav1.CreateOptions{})
		Expect(err).ToNot(HaveOccurred())
		syncStatus(checkAll)

		podHandler.addCallExpected()
		stsHandler.relatedCallExpected()
		istsHandler.relatedCallExpected()
		podStsRelation.addCallExpected()
		_, err = fakeClient.CoreV1().Pods(namespaceDefault).Create(ctx, newPod(namespaceDefault, pod2Name, "apps", stsName), metav1.CreateOptions{})
		Expect(err).ToNot(HaveOccurred())

		Eventually(func(g Gomega) {
			sts, _ := stsStorage.GetNode(types.NamespacedName{Namespace: namespaceDefault, Name: stsName})
			g.Expect(sts).NotTo(BeNil())

			preOrders := sts.GetPreOrders()
			g.Expect(preOrders).To(HaveLen(1))
			postOrders := sts.GetPostOrders()
			g.Expect(postOrders).To(HaveLen(2))
			g.Expect(checkAll()).To(BeTrue())
		}).Should(Succeed())
	})

	It("create pod and sts; delete pod and sts", func() {
		var err error
		pod1Name := "testpod1"
		stsName := "testSts2"

		podHandler.addCallExpected()
		_, err = fakeClient.CoreV1().Pods(namespaceDefault).Create(ctx, newPod(namespaceDefault, pod1Name, "apps", stsName), metav1.CreateOptions{})
		Expect(err).ToNot(HaveOccurred())
		syncStatus(checkAll)

		stsHandler.addCallExpected()
		istsHandler.addCallExpected()
		istsHandler.relatedCallExpected()
		podStsRelation.addCallExpected()
		stsIstsRelation.addCallExpected()
		_, err = fakeClient.AppsV1().StatefulSets(namespaceDefault).Create(ctx, newStatefulSet(namespaceDefault, stsName, "apps", stsName), metav1.CreateOptions{})
		Expect(err).ToNot(HaveOccurred())
		syncStatus(checkAll)

		podHandler.deleteCallExpected()
		stsHandler.relatedCallExpected()
		istsHandler.relatedCallExpected()
		podStsRelation.deleteCallExpected()
		err = fakeClient.CoreV1().Pods(namespaceDefault).Delete(ctx, pod1Name, metav1.DeleteOptions{})
		Expect(err).ToNot(HaveOccurred())
		syncStatus(checkAll)

		stsHandler.deleteCallExpected()
		istsHandler.relatedCallExpected()
		istsHandler.deleteCallExpected()
		stsIstsRelation.deleteCallExpected()
		err = fakeClient.AppsV1().StatefulSets(namespaceDefault).Delete(ctx, stsName, metav1.DeleteOptions{})
		Expect(err).ToNot(HaveOccurred())
		syncStatus(checkAll)

		Eventually(func(g Gomega) {
			sts, _ := stsStorage.GetNode(types.NamespacedName{Namespace: namespaceDefault, Name: stsName})
			g.Expect(sts).To(BeNil())
		}).Should(Succeed())
	})

	It("create pod and sts; delete sts and pod", func() {
		var err error
		pod1Name := "testpod1"
		stsName := "testSts2"

		podHandler.addCallExpected()
		_, err = fakeClient.CoreV1().Pods(namespaceDefault).Create(ctx, newPod(namespaceDefault, pod1Name, "apps", stsName), metav1.CreateOptions{})
		Expect(err).ToNot(HaveOccurred())
		syncStatus(checkAll)

		stsHandler.addCallExpected()
		istsHandler.addCallExpected()
		istsHandler.relatedCallExpected()
		podStsRelation.addCallExpected()
		stsIstsRelation.addCallExpected()
		_, err = fakeClient.AppsV1().StatefulSets(namespaceDefault).Create(ctx, newStatefulSet(namespaceDefault, stsName, "apps", stsName), metav1.CreateOptions{})
		Expect(err).ToNot(HaveOccurred())
		syncStatus(checkAll)

		stsHandler.deleteCallExpected()
		istsHandler.relatedCallExpected()
		istsHandler.deleteCallExpected()
		podStsRelation.deleteCallExpected()
		stsIstsRelation.deleteCallExpected()
		err = fakeClient.AppsV1().StatefulSets(namespaceDefault).Delete(ctx, stsName, metav1.DeleteOptions{})
		Expect(err).ToNot(HaveOccurred())
		syncStatus(checkAll)

		podHandler.deleteCallExpected()
		err = fakeClient.CoreV1().Pods(namespaceDefault).Delete(ctx, pod1Name, metav1.DeleteOptions{})
		Expect(err).ToNot(HaveOccurred())
		syncStatus(checkAll)

		Eventually(func(g Gomega) {
			pod, _ := podStorage.GetNode(types.NamespacedName{Namespace: namespaceDefault, Name: pod1Name})
			sts, _ := stsStorage.GetNode(types.NamespacedName{Namespace: namespaceDefault, Name: stsName})
			g.Expect(sts).To(BeNil())
			g.Expect(pod).To(BeNil())
			g.Expect(checkAll()).To(BeTrue())
		}).Should(Succeed())
	})

	It("create pod and sts; update pod and sts", func() {
		var err error
		pod1Name := "testpod1"
		stsName := "testSts2"

		podHandler.addCallExpected()
		_, err = fakeClient.CoreV1().Pods(namespaceDefault).Create(ctx, newPod(namespaceDefault, pod1Name, "apps", stsName), metav1.CreateOptions{})
		Expect(err).ToNot(HaveOccurred())
		syncStatus(checkAll)

		stsHandler.addCallExpected()
		istsHandler.addCallExpected()
		istsHandler.relatedCallExpected()
		podStsRelation.addCallExpected()
		stsIstsRelation.addCallExpected()
		_, err = fakeClient.AppsV1().StatefulSets(namespaceDefault).Create(ctx, newStatefulSet(namespaceDefault, stsName, "apps", stsName), metav1.CreateOptions{})
		Expect(err).ToNot(HaveOccurred())
		syncStatus(checkAll)

		podHandler.updateCallExpected()
		stsHandler.relatedCallExpected()
		istsHandler.relatedCallExpected()
		_, err = fakeClient.CoreV1().Pods(namespaceDefault).Update(ctx, newPod(namespaceDefault, pod1Name, "apps", stsName), metav1.UpdateOptions{})
		Expect(err).ToNot(HaveOccurred())
		syncStatus(checkAll)

		stsHandler.updateCallExpected()
		istsHandler.relatedCallExpected()
		_, err = fakeClient.AppsV1().StatefulSets(namespaceDefault).Update(ctx, newStatefulSet(namespaceDefault, stsName, "apps", stsName), metav1.UpdateOptions{})
		Expect(err).ToNot(HaveOccurred())
		syncStatus(checkAll)

		Eventually(func(g Gomega) {
			sts, _ := stsStorage.GetNode(types.NamespacedName{Namespace: namespaceDefault, Name: stsName})
			g.Expect(sts).NotTo(BeNil())
			preOrders := sts.GetPreOrders()
			g.Expect(preOrders).To(HaveLen(1))
			postOrders := sts.GetPostOrders()
			g.Expect(postOrders).To(HaveLen(1))
			g.Expect(checkAll()).To(BeTrue())
		}).Should(Succeed())
	})

	It("create pod and sts; update pod label to no longer match", func() {
		var err error
		pod1Name := "testpod1"
		stsName := "testSts2"

		podHandler.addCallExpected()
		_, err = fakeClient.CoreV1().Pods(namespaceDefault).Create(ctx, newPod(namespaceDefault, pod1Name, "apps", stsName), metav1.CreateOptions{})
		Expect(err).ToNot(HaveOccurred())
		syncStatus(checkAll)

		stsHandler.addCallExpected()
		istsHandler.addCallExpected()
		istsHandler.relatedCallExpected()
		podStsRelation.addCallExpected()
		stsIstsRelation.addCallExpected()
		_, err = fakeClient.AppsV1().StatefulSets(namespaceDefault).Create(ctx, newStatefulSet(namespaceDefault, stsName, "apps", stsName), metav1.CreateOptions{})
		Expect(err).ToNot(HaveOccurred())
		syncStatus(checkAll)

		podHandler.updateCallExpected()
		stsHandler.relatedCallExpected()
		istsHandler.relatedCallExpected()
		podStsRelation.deleteCallExpected()
		_, err = fakeClient.CoreV1().Pods(namespaceDefault).Update(ctx, newPod(namespaceDefault, pod1Name, "apps", stsName+"failed"), metav1.UpdateOptions{})
		Expect(err).ToNot(HaveOccurred())

		Eventually(func(g Gomega) {
			sts, _ := stsStorage.GetNode(types.NamespacedName{Namespace: namespaceDefault, Name: stsName})
			g.Expect(sts).NotTo(BeNil())
			preOrders := sts.GetPreOrders()
			g.Expect(preOrders).To(HaveLen(1))
			postOrders := sts.GetPostOrders()
			g.Expect(postOrders).To(BeEmpty())
			g.Expect(checkAll()).To(BeTrue())
		}).Should(Succeed())
	})

	It("create pod and sts; update sts label to no longer match", func() {
		var err error
		pod1Name := "testpod1"
		stsName := "testSts2"

		podHandler.addCallExpected()
		_, err = fakeClient.CoreV1().Pods(namespaceDefault).Create(ctx, newPod(namespaceDefault, pod1Name, "apps", stsName), metav1.CreateOptions{})
		Expect(err).ToNot(HaveOccurred())
		syncStatus(checkAll)

		stsHandler.addCallExpected()
		istsHandler.addCallExpected()
		istsHandler.relatedCallExpected()
		podStsRelation.addCallExpected()
		stsIstsRelation.addCallExpected()
		_, err = fakeClient.AppsV1().StatefulSets(namespaceDefault).Create(ctx, newStatefulSet(namespaceDefault, stsName, "apps", stsName), metav1.CreateOptions{})
		Expect(err).ToNot(HaveOccurred())
		syncStatus(checkAll)

		stsHandler.updateCallExpected()
		istsHandler.relatedCallExpected()
		podStsRelation.deleteCallExpected()

		_, err = fakeClient.AppsV1().StatefulSets(namespaceDefault).Update(ctx, newStatefulSet(namespaceDefault, stsName, "apps", stsName+"failed"), metav1.UpdateOptions{})
		Expect(err).ToNot(HaveOccurred())

		Eventually(func(g Gomega) {
			sts, _ := stsStorage.GetNode(types.NamespacedName{Namespace: namespaceDefault, Name: stsName})
			g.Expect(sts).NotTo(BeNil())
			preOrders := sts.GetPreOrders()
			postOrders := sts.GetPostOrders()
			g.Expect(preOrders).To(HaveLen(1))
			g.Expect(postOrders).To(BeEmpty())
			g.Expect(checkAll()).To(BeTrue())
		}).Should(Succeed())
	})
})

var _ = Describe("test suite with cluster role config(cluster role and direct reference)", func() {
	var manager Manager
	var fakeClient *fake.Clientset
	var clusterRoleBindingHandler, clusterRoleHandler, saHandler *objecthandler
	var saBindingRelation, roleBindingRelation *relationHandler

	var clusterRoleBindingStorage, clusterroleStorage, saStorage TopoNodeStorage
	var ctx context.Context
	var cancel func()

	checkAll := func() bool {
		return clusterRoleBindingHandler.matchExpected() &&
			clusterRoleHandler.matchExpected() &&
			saHandler.matchExpected() &&
			saBindingRelation.matchExpected() &&
			roleBindingRelation.matchExpected()
	}

	BeforeEach(func() {
		fakeClient = fake.NewSimpleClientset()
		k8sInformerFactory := informers.NewSharedInformerFactory(fakeClient, 0)
		var err error
		manager, err = NewResourcesTopoManager(*buildManagerConfig(buildClusterTest(k8sInformerFactory)))
		Expect(err).NotTo(HaveOccurred())

		clusterRoleBindingStorage, _ = manager.GetTopoNodeStorage(ClusterRoleBindingMeta)
		clusterroleStorage, _ = manager.GetTopoNodeStorage(ClusterRoleMeta)
		saStorage, _ = manager.GetTopoNodeStorage(ServiceAccountMeta)

		Expect(clusterRoleBindingStorage).NotTo(BeNil())
		Expect(clusterroleStorage).NotTo(BeNil())
		Expect(saStorage).NotTo(BeNil())

		ctx, cancel = context.WithCancel(context.Background())
		clusterRoleBindingHandler = &objecthandler{}
		Expect(manager.AddNodeHandler(ClusterRoleBindingMeta, clusterRoleBindingHandler)).NotTo(HaveOccurred())
		clusterRoleHandler = &objecthandler{}
		Expect(manager.AddNodeHandler(ClusterRoleMeta, clusterRoleHandler)).NotTo(HaveOccurred())
		saHandler = &objecthandler{}
		Expect(manager.AddNodeHandler(ServiceAccountMeta, saHandler)).NotTo(HaveOccurred())

		roleBindingRelation = &relationHandler{}
		Expect(manager.AddRelationHandler(ClusterRoleBindingMeta, ClusterRoleMeta, roleBindingRelation)).To(Succeed())
		saBindingRelation = &relationHandler{}
		Expect(manager.AddRelationHandler(ClusterRoleBindingMeta, ServiceAccountMeta, saBindingRelation)).To(Succeed())

		manager.Start(ctx.Done())
		k8sInformerFactory.Start(ctx.Done())
		k8sInformerFactory.WaitForCacheSync(ctx.Done())
	})
	AfterEach(func() {
		if !checkAll() {
			klog.Infof("end with object [crb %s, cr %s, sa %s]", clusterRoleBindingHandler.string(), clusterRoleHandler.string(), saHandler.string())
			klog.Infof("end with relation [crbCr %s, crbSa %s]", roleBindingRelation.string(), saBindingRelation.string())
		}
		cancel()
	})

	It("create single clusterRoleBinding", func() {
		ns := "testclusterresource"
		crbName := "crbtest"
		crName := "crName"
		saName := "saName"

		clusterRoleBindingHandler.addCallExpected()
		Expect(fakeClient.RbacV1().ClusterRoleBindings().Create(ctx, newClusterRoleBinding(crbName, crName, []types.NamespacedName{{Name: saName, Namespace: ns}}), metav1.CreateOptions{})).NotTo(BeNil())
		syncStatus(checkAll)

		Eventually(func(g Gomega) {
			crb, _ := clusterRoleBindingStorage.GetNode(types.NamespacedName{Name: crbName})
			g.Expect(crb).NotTo(BeNil())
			g.Expect(crb.GetPostOrders()).To(BeEmpty())
			g.Expect(clusterroleStorage.GetNode(types.NamespacedName{Name: crName})).To(BeNil())
			g.Expect(saStorage.GetNode(types.NamespacedName{Name: saName, Namespace: ns})).To(BeNil())
		}).Should(Succeed())
	})

	It("create serviceAccount, clusterRole and clusterRoleBinding", func() {
		ns := "testclusterresource"
		crbName := "crbtest"
		crName := "crName"
		saName := "saName"

		saHandler.addCallExpected()
		Expect(fakeClient.CoreV1().ServiceAccounts(ns).Create(ctx, newServiceAccount(ns, saName), metav1.CreateOptions{})).NotTo(BeNil())
		syncStatus(checkAll)

		clusterRoleHandler.addCallExpected()
		Expect(fakeClient.RbacV1().ClusterRoles().Create(ctx, newClusterRole(crName), metav1.CreateOptions{})).NotTo(BeNil())
		syncStatus(checkAll)

		clusterRoleBindingHandler.addCallExpected()
		saBindingRelation.addCallExpected()
		roleBindingRelation.addCallExpected()
		Expect(fakeClient.RbacV1().ClusterRoleBindings().Create(ctx, newClusterRoleBinding(crbName, crName, []types.NamespacedName{{Name: saName, Namespace: ns}}), metav1.CreateOptions{})).NotTo(BeNil())
		syncStatus(checkAll)
	})

	It("create clusterRoleBinding and related serviceAccount, clusterRole", func() {
		ns := "testclusterresource"
		crbName := "crbtest"
		crName := "crName"
		saName := "saName"

		clusterRoleBindingHandler.addCallExpected()
		Expect(fakeClient.RbacV1().ClusterRoleBindings().Create(ctx, newClusterRoleBinding(crbName, crName, []types.NamespacedName{{Name: saName, Namespace: ns}}), metav1.CreateOptions{})).NotTo(BeNil())
		syncStatus(checkAll)

		saHandler.addCallExpected()
		clusterRoleBindingHandler.relatedCallExpected()
		saBindingRelation.addCallExpected()
		Expect(fakeClient.CoreV1().ServiceAccounts(ns).Create(ctx, newServiceAccount(ns, saName), metav1.CreateOptions{})).NotTo(BeNil())
		syncStatus(checkAll)

		clusterRoleHandler.addCallExpected()
		clusterRoleBindingHandler.relatedCallExpected()
		roleBindingRelation.addCallExpected()
		Expect(fakeClient.RbacV1().ClusterRoles().Create(ctx, newClusterRole(crName), metav1.CreateOptions{})).NotTo(BeNil())
		syncStatus(checkAll)
	})

	It("create all and delete clusterRole and serviceAccount", func() {
		ns := "testclusterresource"
		crbName := "crbtest"
		crName := "crName"
		saName := "saName"

		clusterRoleBindingHandler.addCallExpected()
		Expect(fakeClient.RbacV1().ClusterRoleBindings().Create(ctx, newClusterRoleBinding(crbName, crName, []types.NamespacedName{{Name: saName, Namespace: ns}}), metav1.CreateOptions{})).NotTo(BeNil())
		syncStatus(checkAll)

		saHandler.addCallExpected()
		clusterRoleBindingHandler.relatedCallExpected()
		saBindingRelation.addCallExpected()
		Expect(fakeClient.CoreV1().ServiceAccounts(ns).Create(ctx, newServiceAccount(ns, saName), metav1.CreateOptions{})).NotTo(BeNil())
		syncStatus(checkAll)

		clusterRoleHandler.addCallExpected()
		clusterRoleBindingHandler.relatedCallExpected()
		roleBindingRelation.addCallExpected()
		Expect(fakeClient.RbacV1().ClusterRoles().Create(ctx, newClusterRole(crName), metav1.CreateOptions{})).NotTo(BeNil())
		syncStatus(checkAll)

		saHandler.deleteCallExpected()
		clusterRoleBindingHandler.relatedCallExpected()
		saBindingRelation.deleteCallExpected()
		Expect(fakeClient.CoreV1().ServiceAccounts(ns).Delete(ctx, saName, metav1.DeleteOptions{})).NotTo(HaveOccurred())
		syncStatus(checkAll)

		clusterRoleHandler.deleteCallExpected()
		clusterRoleBindingHandler.relatedCallExpected()
		roleBindingRelation.deleteCallExpected()
		Expect(fakeClient.RbacV1().ClusterRoles().Delete(ctx, crName, metav1.DeleteOptions{})).NotTo(HaveOccurred())
		syncStatus(checkAll)

		Eventually(func(g Gomega) {
			clusterrolebindingNode, _ := clusterRoleBindingStorage.GetNode(types.NamespacedName{Name: crbName})
			g.Expect(clusterrolebindingNode).NotTo(BeNil())
			g.Expect(clusterrolebindingNode.GetPostOrders()).To(BeEmpty())
		}).Should(Succeed())
	})

	It("create all and delete clusterRoleBinding", func() {
		ns := "testclusterresource"
		crbName := "crbtest"
		crName := "crName"
		saName := "saName"

		clusterRoleBindingHandler.addCallExpected()
		Expect(fakeClient.RbacV1().ClusterRoleBindings().Create(ctx, newClusterRoleBinding(crbName, crName, []types.NamespacedName{{Name: saName, Namespace: ns}}), metav1.CreateOptions{})).NotTo(BeNil())
		syncStatus(checkAll)

		saHandler.addCallExpected()
		clusterRoleBindingHandler.relatedCallExpected()
		saBindingRelation.addCallExpected()
		Expect(fakeClient.CoreV1().ServiceAccounts(ns).Create(ctx, newServiceAccount(ns, saName), metav1.CreateOptions{})).NotTo(BeNil())
		syncStatus(checkAll)

		clusterRoleHandler.addCallExpected()
		clusterRoleBindingHandler.relatedCallExpected()
		roleBindingRelation.addCallExpected()
		Expect(fakeClient.RbacV1().ClusterRoles().Create(ctx, newClusterRole(crName), metav1.CreateOptions{})).NotTo(BeNil())
		syncStatus(checkAll)

		clusterRoleBindingHandler.deleteCallExpected()
		roleBindingRelation.deleteCallExpected()
		saBindingRelation.deleteCallExpected()
		Expect(fakeClient.RbacV1().ClusterRoleBindings().Delete(ctx, crbName, metav1.DeleteOptions{})).NotTo(HaveOccurred())
		syncStatus(checkAll)

		Eventually(func(g Gomega) {
			clusterrolebindingNode, _ := clusterRoleBindingStorage.GetNode(types.NamespacedName{Name: crbName})
			g.Expect(clusterrolebindingNode).To(BeNil())
			clusterrole, _ := clusterroleStorage.GetNode(types.NamespacedName{Name: crName})
			g.Expect(clusterrole).NotTo(BeNil())
		}).Should(Succeed())
	})

	It("create all, delete serviceAccount/clusterRole and create again, should match", func() {
		ns := "testclusterresource"
		crbName := "crbtest"
		crName := "crName"
		saName := "saName"

		clusterRoleBindingHandler.addCallExpected()
		Expect(fakeClient.RbacV1().ClusterRoleBindings().Create(ctx, newClusterRoleBinding(crbName, crName, []types.NamespacedName{{Name: saName, Namespace: ns}}), metav1.CreateOptions{})).NotTo(BeNil())
		syncStatus(checkAll)

		saHandler.addCallExpected()
		clusterRoleBindingHandler.relatedCallExpected()
		saBindingRelation.addCallExpected()
		Expect(fakeClient.CoreV1().ServiceAccounts(ns).Create(ctx, newServiceAccount(ns, saName), metav1.CreateOptions{})).NotTo(BeNil())
		syncStatus(checkAll)

		clusterRoleHandler.addCallExpected()
		clusterRoleBindingHandler.relatedCallExpected()
		roleBindingRelation.addCallExpected()
		Expect(fakeClient.RbacV1().ClusterRoles().Create(ctx, newClusterRole(crName), metav1.CreateOptions{})).NotTo(BeNil())
		syncStatus(checkAll)

		saHandler.deleteCallExpected()
		clusterRoleBindingHandler.relatedCallExpected()
		saBindingRelation.deleteCallExpected()
		Expect(fakeClient.CoreV1().ServiceAccounts(ns).Delete(ctx, saName, metav1.DeleteOptions{})).NotTo(HaveOccurred())
		syncStatus(checkAll)

		clusterRoleHandler.deleteCallExpected()
		clusterRoleBindingHandler.relatedCallExpected()
		roleBindingRelation.deleteCallExpected()
		Expect(fakeClient.RbacV1().ClusterRoles().Delete(ctx, crName, metav1.DeleteOptions{})).NotTo(HaveOccurred())
		syncStatus(checkAll)

		saHandler.addCallExpected()
		clusterRoleBindingHandler.relatedCallExpected()
		saBindingRelation.addCallExpected()
		Expect(fakeClient.CoreV1().ServiceAccounts(ns).Create(ctx, newServiceAccount(ns, saName), metav1.CreateOptions{})).NotTo(BeNil())
		syncStatus(checkAll)

		clusterRoleHandler.addCallExpected()
		clusterRoleBindingHandler.relatedCallExpected()
		roleBindingRelation.addCallExpected()
		Expect(fakeClient.RbacV1().ClusterRoles().Create(ctx, newClusterRole(crName), metav1.CreateOptions{})).NotTo(BeNil())
		syncStatus(checkAll)

		Eventually(func(g Gomega) {
			clusterrolebindingNode, _ := clusterRoleBindingStorage.GetNode(types.NamespacedName{Name: crbName})
			g.Expect(clusterrolebindingNode).NotTo(BeNil())
			postNodes := clusterrolebindingNode.GetPostOrders()
			g.Expect(postNodes).To(HaveLen(2))
		}).Should(Succeed())
	})

	It("create all, delete clusterrolebinding and create again, should match", func() {
		ns := "testclusterresource"
		crbName := "crbtest"
		crName := "crName"
		saName := "saName"

		clusterRoleBindingHandler.addCallExpected()
		Expect(fakeClient.RbacV1().ClusterRoleBindings().Create(ctx, newClusterRoleBinding(crbName, crName, []types.NamespacedName{{Name: saName, Namespace: ns}}), metav1.CreateOptions{})).NotTo(BeNil())
		syncStatus(checkAll)

		saHandler.addCallExpected()
		clusterRoleBindingHandler.relatedCallExpected()
		saBindingRelation.addCallExpected()
		Expect(fakeClient.CoreV1().ServiceAccounts(ns).Create(ctx, newServiceAccount(ns, saName), metav1.CreateOptions{})).NotTo(BeNil())
		syncStatus(checkAll)

		clusterRoleHandler.addCallExpected()
		clusterRoleBindingHandler.relatedCallExpected()
		roleBindingRelation.addCallExpected()
		Expect(fakeClient.RbacV1().ClusterRoles().Create(ctx, newClusterRole(crName), metav1.CreateOptions{})).NotTo(BeNil())
		syncStatus(checkAll)

		clusterRoleBindingHandler.deleteCallExpected()
		roleBindingRelation.deleteCallExpected()
		saBindingRelation.deleteCallExpected()
		Expect(fakeClient.RbacV1().ClusterRoleBindings().Delete(ctx, crbName, metav1.DeleteOptions{})).NotTo(HaveOccurred())
		syncStatus(checkAll)

		clusterRoleBindingHandler.addCallExpected()
		saBindingRelation.addCallExpected()
		roleBindingRelation.addCallExpected()
		Expect(fakeClient.RbacV1().ClusterRoleBindings().Create(ctx, newClusterRoleBinding(crbName, crName, []types.NamespacedName{{Name: saName, Namespace: ns}}), metav1.CreateOptions{})).NotTo(BeNil())
		syncStatus(checkAll)

		Eventually(func(g Gomega) {
			clusterrolebindingNode, _ := clusterRoleBindingStorage.GetNode(types.NamespacedName{Name: crbName})
			g.Expect(clusterrolebindingNode).NotTo(BeNil())
			postNodes := clusterrolebindingNode.GetPostOrders()
			g.Expect(postNodes).To(HaveLen(2))
		}).Should(Succeed())
	})
	It("create all and delete all", func() {
		ns := "testclusterresource"
		crbName := "crbtest"
		crName := "crName"
		saName := "saName"

		clusterRoleBindingHandler.addCallExpected()
		Expect(fakeClient.RbacV1().ClusterRoleBindings().Create(ctx, newClusterRoleBinding(crbName, crName, []types.NamespacedName{{Name: saName, Namespace: ns}}), metav1.CreateOptions{})).NotTo(BeNil())
		syncStatus(checkAll)

		saHandler.addCallExpected()
		clusterRoleBindingHandler.relatedCallExpected()
		saBindingRelation.addCallExpected()
		Expect(fakeClient.CoreV1().ServiceAccounts(ns).Create(ctx, newServiceAccount(ns, saName), metav1.CreateOptions{})).NotTo(BeNil())
		syncStatus(checkAll)

		clusterRoleHandler.addCallExpected()
		clusterRoleBindingHandler.relatedCallExpected()
		roleBindingRelation.addCallExpected()
		Expect(fakeClient.RbacV1().ClusterRoles().Create(ctx, newClusterRole(crName), metav1.CreateOptions{})).NotTo(BeNil())
		syncStatus(checkAll)

		saHandler.deleteCallExpected()
		clusterRoleBindingHandler.relatedCallExpected()
		saBindingRelation.deleteCallExpected()
		Expect(fakeClient.CoreV1().ServiceAccounts(ns).Delete(ctx, saName, metav1.DeleteOptions{})).NotTo(HaveOccurred())
		syncStatus(checkAll)

		clusterRoleHandler.deleteCallExpected()
		clusterRoleBindingHandler.relatedCallExpected()
		roleBindingRelation.deleteCallExpected()
		Expect(fakeClient.RbacV1().ClusterRoles().Delete(ctx, crName, metav1.DeleteOptions{})).NotTo(HaveOccurred())
		syncStatus(checkAll)

		clusterRoleBindingHandler.deleteCallExpected()
		Expect(fakeClient.RbacV1().ClusterRoleBindings().Delete(ctx, crbName, metav1.DeleteOptions{})).NotTo(HaveOccurred())
		syncStatus(checkAll)

		Eventually(func(g Gomega) {
			g.Expect(clusterRoleBindingStorage.GetNode(types.NamespacedName{Name: crbName})).To(BeNil())
			g.Expect(clusterroleStorage.GetNode(types.NamespacedName{Name: crName})).To(BeNil())
			g.Expect(saStorage.GetNode(types.NamespacedName{Name: saName, Namespace: ns})).To(BeNil())
		}).Should(Succeed())
	})
})

var _ = Describe("test suite with svc and pod config(label selector and reverse notice configured)", func() {
	var manager Manager
	var fakeClient *fake.Clientset
	var podHandler, svcHandler *objecthandler
	var podSvcRelation *relationHandler

	var podStorage, svcStorage TopoNodeStorage
	var ctx context.Context
	var cancel func()

	checkAll := func() bool {
		return podHandler.matchExpected() &&
			svcHandler.matchExpected() &&
			podSvcRelation.matchExpected()
	}

	BeforeEach(func() {
		fakeClient = fake.NewSimpleClientset()
		k8sInformerFactory := informers.NewSharedInformerFactory(fakeClient, 0)
		var err error
		manager, err = NewResourcesTopoManager(*buildManagerConfig(buildSvcPodTest(k8sInformerFactory)))
		Expect(err).NotTo(HaveOccurred())
		podStorage, _ = manager.GetTopoNodeStorage(PodMeta)
		Expect(podStorage).NotTo(BeNil())
		svcStorage, _ = manager.GetTopoNodeStorage(ServiceMeta)
		Expect(svcStorage).NotTo(BeNil())

		ctx, cancel = context.WithCancel(context.Background())
		podHandler = &objecthandler{}
		Expect(manager.AddNodeHandler(PodMeta, podHandler)).To(Succeed())
		svcHandler = &objecthandler{}
		Expect(manager.AddNodeHandler(ServiceMeta, svcHandler)).To(Succeed())

		podSvcRelation = &relationHandler{}
		Expect(manager.AddRelationHandler(ServiceMeta, PodMeta, podSvcRelation)).To(Succeed())

		manager.Start(ctx.Done())
		k8sInformerFactory.Start(ctx.Done())
		k8sInformerFactory.WaitForCacheSync(ctx.Done())
	})
	AfterEach(func() {
		if !checkAll() {
			klog.Infof("end with object [pod %s, svc %s]", podHandler.string(), svcHandler.string())
			klog.Infof("end with relation [podSvc %s]", podSvcRelation.string())
		}
		cancel()
	})

	It("create pod and svc", func() {
		var err error
		podName := "test1"
		svcName := "testSvc"

		podHandler.addCallExpected()
		_, err = fakeClient.CoreV1().Pods(namespaceDefault).Create(ctx, newPod(namespaceDefault, podName, "apps", podName), metav1.CreateOptions{})
		Expect(err).ToNot(HaveOccurred())
		syncStatus(checkAll)

		svcHandler.addCallExpected()
		podHandler.relatedCallExpected()
		podSvcRelation.addCallExpected()
		_, err = fakeClient.CoreV1().Services(namespaceDefault).Create(ctx, newSvc(namespaceDefault, svcName, "apps", podName), metav1.CreateOptions{})
		Expect(err).ToNot(HaveOccurred())

		Eventually(func(g Gomega) {
			pod, _ := podStorage.GetNode(types.NamespacedName{Namespace: namespaceDefault, Name: podName})
			g.Expect(pod).NotTo(BeNil())
			g.Expect(pod.GetPostOrders()).To(BeEmpty())
			g.Expect(pod.GetPreOrders()).To(HaveLen(1))

			g.Expect(pod.TypeInfo()).To(Equal(PodMeta))
			g.Expect(pod.NodeInfo()).To(Equal(types.NamespacedName{Namespace: namespaceDefault, Name: podName}))

			g.Expect(checkAll()).To(BeTrue())
		}).Should(Succeed())
	})

	It("create svc and pod", func() {
		var err error
		podName := "testPod"
		svcName := "testSvc"

		svcHandler.addCallExpected()
		_, err = fakeClient.CoreV1().Services(namespaceDefault).Create(ctx, newSvc(namespaceDefault, svcName, "apps", podName), metav1.CreateOptions{})
		Expect(err).ToNot(HaveOccurred())
		syncStatus(checkAll)

		podHandler.addCallExpected()
		podSvcRelation.addCallExpected()
		_, err = fakeClient.CoreV1().Pods(namespaceDefault).Create(ctx, newPod(namespaceDefault, podName, "apps", podName), metav1.CreateOptions{})
		Expect(err).ToNot(HaveOccurred())

		Eventually(func(g Gomega) {
			svcNode, _ := svcStorage.GetNode(types.NamespacedName{Namespace: namespaceDefault, Name: svcName})
			g.Expect(svcNode).NotTo(BeNil())
			g.Expect(svcNode.GetPreOrders()).To(BeEmpty())
			g.Expect(svcNode.GetPostOrders()).To(HaveLen(1))

			g.Expect(svcNode.TypeInfo()).To(Equal(ServiceMeta))
			g.Expect(svcNode.NodeInfo()).To(Equal(types.NamespacedName{Namespace: namespaceDefault, Name: svcName}))

			g.Expect(checkAll()).To(BeTrue())
		}).Should(Succeed())
	})

	It("create svc and two pods", func() {
		var err error
		podName := "testPod"
		pod2Name := "testPod2"
		svcName := "testSvc"

		svcHandler.addCallExpected()
		_, err = fakeClient.CoreV1().Services(namespaceDefault).Create(ctx, newSvc(namespaceDefault, svcName, "apps", svcName), metav1.CreateOptions{})
		Expect(err).ToNot(HaveOccurred())
		syncStatus(checkAll)

		podHandler.addCallExpected()
		podSvcRelation.addCallExpected()
		_, err = fakeClient.CoreV1().Pods(namespaceDefault).Create(ctx, newPod(namespaceDefault, podName, "apps", svcName), metav1.CreateOptions{})
		Expect(err).ToNot(HaveOccurred())
		syncStatus(checkAll)

		podHandler.addCallExpected()
		podSvcRelation.addCallExpected()
		_, err = fakeClient.CoreV1().Pods(namespaceDefault).Create(ctx, newPod(namespaceDefault, pod2Name, "apps", svcName), metav1.CreateOptions{})
		Expect(err).ToNot(HaveOccurred())

		Eventually(func(g Gomega) {
			svcNode, _ := svcStorage.GetNode(types.NamespacedName{Namespace: namespaceDefault, Name: svcName})
			g.Expect(svcNode).NotTo(BeNil())
			g.Expect(svcNode.GetPreOrders()).To(BeEmpty())
			g.Expect(svcNode.GetPostOrders()).To(HaveLen(2))

			g.Expect(svcNode.TypeInfo()).To(Equal(ServiceMeta))
			g.Expect(svcNode.NodeInfo()).To(Equal(types.NamespacedName{Namespace: namespaceDefault, Name: svcName}))

			g.Expect(checkAll()).To(BeTrue())
		}).Should(Succeed())
	})

	It("create pod, svc and pod", func() {
		var err error
		podName := "testPod"
		pod2Name := "testPod2"
		svcName := "testSvc"

		podHandler.addCallExpected()
		_, err = fakeClient.CoreV1().Pods(namespaceDefault).Create(ctx, newPod(namespaceDefault, podName, "apps", svcName), metav1.CreateOptions{})
		Expect(err).ToNot(HaveOccurred())
		syncStatus(checkAll)

		svcHandler.addCallExpected()
		podHandler.relatedCallExpected()
		podSvcRelation.addCallExpected()
		_, err = fakeClient.CoreV1().Services(namespaceDefault).Create(ctx, newSvc(namespaceDefault, svcName, "apps", svcName), metav1.CreateOptions{})
		Expect(err).ToNot(HaveOccurred())
		syncStatus(checkAll)

		podHandler.addCallExpected()
		podSvcRelation.addCallExpected()
		_, err = fakeClient.CoreV1().Pods(namespaceDefault).Create(ctx, newPod(namespaceDefault, pod2Name, "apps", svcName), metav1.CreateOptions{})
		Expect(err).ToNot(HaveOccurred())

		Eventually(func(g Gomega) {
			svcNode, _ := svcStorage.GetNode(types.NamespacedName{Namespace: namespaceDefault, Name: svcName})
			g.Expect(svcNode).NotTo(BeNil())
			g.Expect(svcNode.GetPreOrders()).To(BeEmpty())
			g.Expect(svcNode.GetPostOrders()).To(HaveLen(2))

			g.Expect(svcNode.TypeInfo()).To(Equal(ServiceMeta))
			g.Expect(svcNode.NodeInfo()).To(Equal(types.NamespacedName{Namespace: namespaceDefault, Name: svcName}))

			g.Expect(checkAll()).To(BeTrue())
		}).Should(Succeed())
	})

	It("create svc and no relation pod", func() {
		var err error
		podName := "testPod"
		pod2Name := "testPod2"
		svcName := "testSvc"

		podHandler.addCallExpected()
		_, err = fakeClient.CoreV1().Pods(namespaceDefault).Create(ctx, newPod(namespaceDefault, podName, "apps", podName), metav1.CreateOptions{})
		Expect(err).ToNot(HaveOccurred())
		syncStatus(checkAll)

		svcHandler.addCallExpected()
		_, err = fakeClient.CoreV1().Services(namespaceDefault).Create(ctx, newSvc(namespaceDefault, svcName, "apps", svcName), metav1.CreateOptions{})
		Expect(err).ToNot(HaveOccurred())
		syncStatus(checkAll)

		podHandler.addCallExpected()
		_, err = fakeClient.CoreV1().Pods(namespaceDefault).Create(ctx, newPod(namespaceDefault, pod2Name, "apps", pod2Name), metav1.CreateOptions{})
		Expect(err).ToNot(HaveOccurred())
		syncStatus(checkAll)
	})

	It("create two svc-pod group", func() {
		var err error
		podName := "testPod"
		podName2 := "testPod2"
		podName3 := "testPod3"
		svcName := "testSvc"
		svcName2 := "testSvc2"

		podHandler.addCallExpected()
		_, err = fakeClient.CoreV1().Pods(namespaceDefault).Create(ctx, newPod(namespaceDefault, podName, "app1", svcName), metav1.CreateOptions{})
		Expect(err).ToNot(HaveOccurred())
		syncStatus(checkAll)

		podHandler.addCallExpected()
		_, err = fakeClient.CoreV1().Pods(namespaceDefault).Create(ctx, newPod(namespaceDefault, podName2, "app2", svcName2), metav1.CreateOptions{})
		Expect(err).ToNot(HaveOccurred())
		syncStatus(checkAll)

		svcHandler.addCallExpected()
		podSvcRelation.addCallExpected()
		podHandler.relatedCallExpected()
		_, err = fakeClient.CoreV1().Services(namespaceDefault).Create(ctx, newSvc(namespaceDefault, svcName, "app1", svcName), metav1.CreateOptions{})
		Expect(err).ToNot(HaveOccurred())
		syncStatus(checkAll)

		svcHandler.addCallExpected()
		podSvcRelation.addCallExpected()
		podHandler.relatedCallExpected()
		_, err = fakeClient.CoreV1().Services(namespaceDefault).Create(ctx, newSvc(namespaceDefault, svcName2, "app2", svcName2), metav1.CreateOptions{})
		Expect(err).ToNot(HaveOccurred())
		syncStatus(checkAll)

		podHandler.addCallExpected()
		podSvcRelation.addCallExpected()
		podSvcRelation.addCallExpected()
		_, err = fakeClient.CoreV1().Pods(namespaceDefault).Create(ctx, newPod(namespaceDefault, podName3, "app2", svcName2, "app1", svcName), metav1.CreateOptions{})
		Expect(err).ToNot(HaveOccurred())
		syncStatus(checkAll)
	})

	It("create pod and svc; delete pod", func() {
		var err error
		podName := "test1"
		svcName := "testSvc"

		podHandler.addCallExpected()
		_, err = fakeClient.CoreV1().Pods(namespaceDefault).Create(ctx, newPod(namespaceDefault, podName, "apps", podName), metav1.CreateOptions{})
		Expect(err).ToNot(HaveOccurred())
		syncStatus(checkAll)

		svcHandler.addCallExpected()
		podHandler.relatedCallExpected()
		podSvcRelation.addCallExpected()
		_, err = fakeClient.CoreV1().Services(namespaceDefault).Create(ctx, newSvc(namespaceDefault, svcName, "apps", podName), metav1.CreateOptions{})
		Expect(err).ToNot(HaveOccurred())
		syncStatus(checkAll)

		svcHandler.deleteCallExpected()
		podSvcRelation.deleteCallExpected()
		podHandler.relatedCallExpected()
		Expect(fakeClient.CoreV1().Services(namespaceDefault).Delete(ctx, svcName, metav1.DeleteOptions{})).To(Succeed())
		syncStatus(checkAll)
	})

	It("create pod and svc; delete svc", func() {
		var err error
		podName := "test1"
		svcName := "testSvc"

		podHandler.addCallExpected()
		_, err = fakeClient.CoreV1().Pods(namespaceDefault).Create(ctx, newPod(namespaceDefault, podName, "apps", podName), metav1.CreateOptions{})
		Expect(err).ToNot(HaveOccurred())
		syncStatus(checkAll)

		svcHandler.addCallExpected()
		podHandler.relatedCallExpected()
		podSvcRelation.addCallExpected()
		_, err = fakeClient.CoreV1().Services(namespaceDefault).Create(ctx, newSvc(namespaceDefault, svcName, "apps", podName), metav1.CreateOptions{})
		Expect(err).ToNot(HaveOccurred())
		syncStatus(checkAll)

		podHandler.deleteCallExpected()
		podSvcRelation.deleteCallExpected()
		Expect(fakeClient.CoreV1().Pods(namespaceDefault).Delete(ctx, podName, metav1.DeleteOptions{})).To(Succeed())
		syncStatus(checkAll)
	})

	It("create pod and svc; update svc", func() {
		var err error
		podName := "test1"
		svcName := "testSvc"

		podHandler.addCallExpected()
		_, err = fakeClient.CoreV1().Pods(namespaceDefault).Create(ctx, newPod(namespaceDefault, podName, "apps", podName), metav1.CreateOptions{})
		Expect(err).ToNot(HaveOccurred())
		syncStatus(checkAll)

		svcHandler.addCallExpected()
		podHandler.relatedCallExpected()
		podSvcRelation.addCallExpected()
		_, err = fakeClient.CoreV1().Services(namespaceDefault).Create(ctx, newSvc(namespaceDefault, svcName, "apps", podName), metav1.CreateOptions{})
		Expect(err).ToNot(HaveOccurred())
		syncStatus(checkAll)

		svcHandler.updateCallExpected()
		podHandler.relatedCallExpected()
		_, err = fakeClient.CoreV1().Services(namespaceDefault).Update(ctx, newSvc(namespaceDefault, svcName, "apps", podName), metav1.UpdateOptions{})
		Expect(err).ToNot(HaveOccurred())
		syncStatus(checkAll)
	})

	It("create pod and svc; update svc selector to no longer match", func() {
		var err error
		podName := "test1"
		svcName := "testSvc"

		podHandler.addCallExpected()
		_, err = fakeClient.CoreV1().Pods(namespaceDefault).Create(ctx, newPod(namespaceDefault, podName, "apps", podName), metav1.CreateOptions{})
		Expect(err).ToNot(HaveOccurred())
		syncStatus(checkAll)

		svcHandler.addCallExpected()
		podHandler.relatedCallExpected()
		podSvcRelation.addCallExpected()
		_, err = fakeClient.CoreV1().Services(namespaceDefault).Create(ctx, newSvc(namespaceDefault, svcName, "apps", podName), metav1.CreateOptions{})
		Expect(err).ToNot(HaveOccurred())
		syncStatus(checkAll)

		svcHandler.updateCallExpected()
		podHandler.relatedCallExpected()
		podSvcRelation.deleteCallExpected()
		_, err = fakeClient.CoreV1().Services(namespaceDefault).Update(ctx, newSvc(namespaceDefault, svcName, "apps", podName+"failed"), metav1.UpdateOptions{})
		Expect(err).ToNot(HaveOccurred())
		syncStatus(checkAll)
	})
})

var _ = Describe("test suite with deploy config(label selector and owner reference configured)", func() {
	var manager Manager
	var fakeClient *fake.Clientset
	var podHandler, replicasetHandler, deployHandler *objecthandler
	var podRsRelation, rsDeployRelation *relationHandler

	var podStorage, replicasetStorage, deployStorage TopoNodeStorage
	var ctx context.Context
	var cancel func()

	checkAll := func() bool {
		return podHandler.matchExpected() &&
			replicasetHandler.matchExpected() &&
			deployHandler.matchExpected() &&
			podRsRelation.matchExpected() &&
			rsDeployRelation.matchExpected()
	}

	BeforeEach(func() {
		fakeClient = fake.NewSimpleClientset()
		k8sInformerFactory := informers.NewSharedInformerFactory(fakeClient, 0)
		var err error
		manager, err = NewResourcesTopoManager(*buildManagerConfig(buildDeployTopoConfig(k8sInformerFactory)))
		Expect(err).NotTo(HaveOccurred())
		replicasetStorage, _ = manager.GetTopoNodeStorage(ReplicaSetMeta)
		podStorage, _ = manager.GetTopoNodeStorage(PodMeta)
		deployStorage, _ = manager.GetTopoNodeStorage(DeployMeta)
		Expect(replicasetStorage).NotTo(BeNil())
		Expect(podStorage).NotTo(BeNil())
		Expect(deployStorage).NotTo(BeNil())

		ctx, cancel = context.WithCancel(context.Background())
		podHandler = &objecthandler{}
		Expect(manager.AddNodeHandler(PodMeta, podHandler)).Should(Succeed())
		replicasetHandler = &objecthandler{}
		Expect(manager.AddNodeHandler(ReplicaSetMeta, replicasetHandler)).Should(Succeed())
		deployHandler = &objecthandler{}
		Expect(manager.AddNodeHandler(DeployMeta, deployHandler)).Should(Succeed())

		podRsRelation = &relationHandler{}
		Expect(manager.AddRelationHandler(ReplicaSetMeta, PodMeta, podRsRelation)).Should(Succeed())
		rsDeployRelation = &relationHandler{}
		Expect(manager.AddRelationHandler(DeployMeta, ReplicaSetMeta, rsDeployRelation)).Should(Succeed())

		manager.Start(ctx.Done())
		k8sInformerFactory.Start(ctx.Done())
		k8sInformerFactory.WaitForCacheSync(ctx.Done())
	})
	AfterEach(func() {
		if !checkAll() {
			klog.Infof("end with object [rs %s, pod %s, deploy %s]", replicasetHandler.string(), podHandler.string(), deployHandler.string())
			klog.Infof("end with relation [rsDeploy %s, podRs %s]", rsDeployRelation.string(), podRsRelation.string())
		}
		cancel()
	})

	It("create objects with label owner match", func() {
		deployName := "testDeploy"
		rsName := deployName + "zxcvb"
		podName := rsName + "asdfg"
		labels := []string{"apps", deployName}
		var err error

		deployHandler.addCallExpected()
		_, err = fakeClient.AppsV1().Deployments(namespaceDefault).Create(ctx, newDeploy(namespaceDefault, deployName, labels...), metav1.CreateOptions{})
		Expect(err).ToNot(HaveOccurred())
		syncStatus(checkAll)

		replicasetHandler.addCallExpected()
		rsDeployRelation.addCallExpected()
		deployHandler.relatedCallExpected()
		rs := newReplicaSet(namespaceDefault, rsName, labels...)
		setOwner(rs, DeployMeta, deployName)
		Expect(fakeClient.AppsV1().ReplicaSets(namespaceDefault).Create(ctx, rs, metav1.CreateOptions{})).NotTo(BeNil())
		syncStatus(checkAll)

		podHandler.addCallExpected()
		podRsRelation.addCallExpected()
		replicasetHandler.relatedCallExpected()
		deployHandler.relatedCallExpected()
		pod := newPod(namespaceDefault, podName, labels...)
		setOwner(pod, ReplicaSetMeta, rsName)
		Expect(fakeClient.CoreV1().Pods(namespaceDefault).Create(ctx, pod, metav1.CreateOptions{})).NotTo(BeNil())
		syncStatus(checkAll)
	})

	It("create objects with label owner match(2)", func() {
		deployName := "testDeploy"
		rsName := deployName + "zxcvb"
		podName := rsName + "asdfg"
		labels := []string{"apps", deployName}
		var err error

		podHandler.addCallExpected()
		pod := newPod(namespaceDefault, podName, labels...)
		setOwner(pod, ReplicaSetMeta, rsName)
		Expect(fakeClient.CoreV1().Pods(namespaceDefault).Create(ctx, pod, metav1.CreateOptions{})).NotTo(BeNil())
		syncStatus(checkAll)

		replicasetHandler.addCallExpected()
		podRsRelation.addCallExpected()
		rs := newReplicaSet(namespaceDefault, rsName, labels...)
		setOwner(rs, DeployMeta, deployName)
		Expect(fakeClient.AppsV1().ReplicaSets(namespaceDefault).Create(ctx, rs, metav1.CreateOptions{})).NotTo(BeNil())
		syncStatus(checkAll)

		deployHandler.addCallExpected()
		rsDeployRelation.addCallExpected()
		_, err = fakeClient.AppsV1().Deployments(namespaceDefault).Create(ctx, newDeploy(namespaceDefault, deployName, labels...), metav1.CreateOptions{})
		Expect(err).ToNot(HaveOccurred())
		syncStatus(checkAll)
	})

	It("create objects with label match but owner not match", func() {
		deployName := "testDeploy"
		rsName := deployName + "zxcvb"
		podName := rsName + "asdfg"
		labels := []string{"apps", deployName}
		var err error

		deployHandler.addCallExpected()
		_, err = fakeClient.AppsV1().Deployments(namespaceDefault).Create(ctx, newDeploy(namespaceDefault, deployName, labels...), metav1.CreateOptions{})
		Expect(err).ToNot(HaveOccurred())
		syncStatus(checkAll)

		replicasetHandler.addCallExpected()
		rsDeployRelation.addCallExpected()
		deployHandler.relatedCallExpected()
		rs := newReplicaSet(namespaceDefault, rsName, labels...)
		setOwner(rs, DeployMeta, deployName)
		Expect(fakeClient.AppsV1().ReplicaSets(namespaceDefault).Create(ctx, rs, metav1.CreateOptions{})).NotTo(BeNil())
		syncStatus(checkAll)

		podHandler.addCallExpected()
		pod := newPod(namespaceDefault, podName, labels...)
		setOwner(pod, ReplicaSetMeta, rsName+"failed")
		Expect(fakeClient.CoreV1().Pods(namespaceDefault).Create(ctx, pod, metav1.CreateOptions{})).NotTo(BeNil())
		syncStatus(checkAll)
	})

	It("create objects with label match but owner not match(2)", func() {
		deployName := "testDeploy"
		rsName := deployName + "zxcvb"
		podName := rsName + "asdfg"
		labels := []string{"apps", deployName}
		var err error

		replicasetHandler.addCallExpected()
		rs := newReplicaSet(namespaceDefault, rsName, labels...)
		setOwner(rs, DeployMeta, deployName)
		Expect(fakeClient.AppsV1().ReplicaSets(namespaceDefault).Create(ctx, rs, metav1.CreateOptions{})).NotTo(BeNil())
		syncStatus(checkAll)

		podHandler.addCallExpected()
		podRsRelation.addCallExpected()
		replicasetHandler.relatedCallExpected()
		pod := newPod(namespaceDefault, podName, labels...)
		setOwner(pod, ReplicaSetMeta, rsName)
		Expect(fakeClient.CoreV1().Pods(namespaceDefault).Create(ctx, pod, metav1.CreateOptions{})).NotTo(BeNil())
		syncStatus(checkAll)

		deployHandler.addCallExpected()
		_, err = fakeClient.AppsV1().Deployments(namespaceDefault).Create(ctx, newDeploy(namespaceDefault, deployName+"failed", labels...), metav1.CreateOptions{})
		Expect(err).ToNot(HaveOccurred())
		syncStatus(checkAll)
	})

	It("create two deploy with same label config", func() {
		deployName := "testDeploy"
		rsName := deployName + "zxcvb"
		podName1 := rsName + "asdfg"
		podName2 := rsName + "asdfh"

		deployName2 := "testDeploy2"
		rsName2 := deployName2 + "zxcvc"
		podName21 := rsName2 + "asdfi"

		labels := []string{"apps", deployName}
		var err error

		deployHandler.addCallExpected()
		_, err = fakeClient.AppsV1().Deployments(namespaceDefault).Create(ctx, newDeploy(namespaceDefault, deployName, labels...), metav1.CreateOptions{})
		Expect(err).ToNot(HaveOccurred())
		syncStatus(checkAll)

		replicasetHandler.addCallExpected()
		rsDeployRelation.addCallExpected()
		deployHandler.relatedCallExpected()
		rs := newReplicaSet(namespaceDefault, rsName, labels...)
		setOwner(rs, DeployMeta, deployName)
		Expect(fakeClient.AppsV1().ReplicaSets(namespaceDefault).Create(ctx, rs, metav1.CreateOptions{})).NotTo(BeNil())
		syncStatus(checkAll)

		podHandler.addCallExpected()
		podRsRelation.addCallExpected()
		replicasetHandler.relatedCallExpected()
		deployHandler.relatedCallExpected()
		pod := newPod(namespaceDefault, podName1, labels...)
		setOwner(pod, ReplicaSetMeta, rsName)
		Expect(fakeClient.CoreV1().Pods(namespaceDefault).Create(ctx, pod, metav1.CreateOptions{})).NotTo(BeNil())
		syncStatus(checkAll)

		podHandler.addCallExpected()
		podRsRelation.addCallExpected()
		replicasetHandler.relatedCallExpected()
		deployHandler.relatedCallExpected()
		pod2 := newPod(namespaceDefault, podName2, labels...)
		setOwner(pod2, ReplicaSetMeta, rsName)
		Expect(fakeClient.CoreV1().Pods(namespaceDefault).Create(ctx, pod2, metav1.CreateOptions{})).NotTo(BeNil())
		syncStatus(checkAll)

		podHandler.addCallExpected()
		pod21 := newPod(namespaceDefault, podName21, labels...)
		setOwner(pod21, ReplicaSetMeta, rsName2)
		Expect(fakeClient.CoreV1().Pods(namespaceDefault).Create(ctx, pod21, metav1.CreateOptions{})).NotTo(BeNil())
		syncStatus(checkAll)

		replicasetHandler.addCallExpected()
		podRsRelation.addCallExpected()
		rs2 := newReplicaSet(namespaceDefault, rsName2, labels...)
		setOwner(rs2, DeployMeta, deployName2)
		Expect(fakeClient.AppsV1().ReplicaSets(namespaceDefault).Create(ctx, rs2, metav1.CreateOptions{})).NotTo(BeNil())
		syncStatus(checkAll)

		deployHandler.addCallExpected()
		rsDeployRelation.addCallExpected()
		_, err = fakeClient.AppsV1().Deployments(namespaceDefault).Create(ctx, newDeploy(namespaceDefault, deployName2, labels...), metav1.CreateOptions{})
		Expect(err).ToNot(HaveOccurred())
		syncStatus(checkAll)

		Eventually(func(g Gomega) {
			rs1, _ := replicasetStorage.GetNode(types.NamespacedName{Name: rsName, Namespace: namespaceDefault})
			Expect(rs1).NotTo(BeNil())
			Expect(rs1.GetPreOrders()).To(HaveLen(1))
			Expect(rs1.GetPostOrders()).To(HaveLen(2))

			rs2, _ := replicasetStorage.GetNode(types.NamespacedName{Name: rsName2, Namespace: namespaceDefault})
			Expect(rs2).NotTo(BeNil())
			Expect(rs2.GetPreOrders()).To(HaveLen(1))
			Expect(rs2.GetPostOrders()).To(HaveLen(1))
		}).Should(Succeed())
	})
})

var _ = Describe("test suite with labelRelations update", func() {
	var manager Manager
	var fakeClient *fake.Clientset
	var clusterRoleBindingHandler, clusterRoleHandler, saHandler *objecthandler
	var saBindingRelation, roleBindingRelation *relationHandler

	var clusterRoleBindingStorage, clusterroleStorage, saStorage TopoNodeStorage
	var ctx context.Context
	var cancel func()

	checkAll := func() bool {
		return clusterRoleBindingHandler.matchExpected() &&
			clusterRoleHandler.matchExpected() &&
			saHandler.matchExpected() &&
			saBindingRelation.matchExpected() &&
			roleBindingRelation.matchExpected()
	}

	BeforeEach(func() {
		fakeClient = fake.NewSimpleClientset()
		k8sInformerFactory := informers.NewSharedInformerFactory(fakeClient, 0)
		var err error
		manager, err = NewResourcesTopoManager(*buildManagerConfig(buildClusterTest(k8sInformerFactory)))
		Expect(err).NotTo(HaveOccurred())

		clusterRoleBindingStorage, _ = manager.GetTopoNodeStorage(ClusterRoleBindingMeta)
		clusterroleStorage, _ = manager.GetTopoNodeStorage(ClusterRoleMeta)
		saStorage, _ = manager.GetTopoNodeStorage(ServiceAccountMeta)

		Expect(clusterRoleBindingStorage).NotTo(BeNil())
		Expect(clusterroleStorage).NotTo(BeNil())
		Expect(saStorage).NotTo(BeNil())

		ctx, cancel = context.WithCancel(context.Background())
		clusterRoleBindingHandler = &objecthandler{}
		Expect(manager.AddNodeHandler(ClusterRoleBindingMeta, clusterRoleBindingHandler)).NotTo(HaveOccurred())
		clusterRoleHandler = &objecthandler{}
		Expect(manager.AddNodeHandler(ClusterRoleMeta, clusterRoleHandler)).NotTo(HaveOccurred())
		saHandler = &objecthandler{}
		Expect(manager.AddNodeHandler(ServiceAccountMeta, saHandler)).NotTo(HaveOccurred())

		roleBindingRelation = &relationHandler{}
		Expect(manager.AddRelationHandler(ClusterRoleBindingMeta, ClusterRoleMeta, roleBindingRelation)).To(Succeed())
		saBindingRelation = &relationHandler{}
		Expect(manager.AddRelationHandler(ClusterRoleBindingMeta, ServiceAccountMeta, saBindingRelation)).To(Succeed())

		manager.Start(ctx.Done())
		k8sInformerFactory.Start(ctx.Done())
		k8sInformerFactory.WaitForCacheSync(ctx.Done())
	})
	AfterEach(func() {
		if !checkAll() {
			klog.Infof("end with object [crb %s, cr %s, sa %s]", clusterRoleBindingHandler.string(), clusterRoleHandler.string(), saHandler.string())
			klog.Infof("end with relation [crbCr %s, crbSa %s]", roleBindingRelation.string(), saBindingRelation.string())
		}
		cancel()
	})

	It("create two serviceAccounts, clusterRole and clusterRoleBinding", func() {
		ns := "testclusterresource"
		crbName := "crbtest"
		crName := "crName"
		saName := "saName"
		saName2 := "saName2"

		saHandler.addCallExpected()
		Expect(fakeClient.CoreV1().ServiceAccounts(ns).Create(ctx, newServiceAccount(ns, saName), metav1.CreateOptions{})).NotTo(BeNil())
		saHandler.addCallExpected()
		Expect(fakeClient.CoreV1().ServiceAccounts(ns).Create(ctx, newServiceAccount(ns, saName2), metav1.CreateOptions{})).NotTo(BeNil())
		syncStatus(checkAll)

		clusterRoleHandler.addCallExpected()
		Expect(fakeClient.RbacV1().ClusterRoles().Create(ctx, newClusterRole(crName), metav1.CreateOptions{})).NotTo(BeNil())
		syncStatus(checkAll)

		clusterRoleBindingHandler.addCallExpected()
		saBindingRelation.addCallExpected()
		saBindingRelation.addCallExpected()
		roleBindingRelation.addCallExpected()
		Expect(fakeClient.RbacV1().ClusterRoleBindings().Create(ctx, newClusterRoleBinding(crbName, crName,
			[]types.NamespacedName{{Name: saName, Namespace: ns}, {Name: saName2, Namespace: ns}}),
			metav1.CreateOptions{})).NotTo(BeNil())
		syncStatus(checkAll)
	})

	It("create all, clear crb spec, and reset", func() {
		ns := "testclusterresource"
		crbName := "crbtest"
		crName := "crName"
		saName := "saName"
		saName2 := "saName2"

		saHandler.addCallExpected()
		Expect(fakeClient.CoreV1().ServiceAccounts(ns).Create(ctx, newServiceAccount(ns, saName), metav1.CreateOptions{})).NotTo(BeNil())
		saHandler.addCallExpected()
		Expect(fakeClient.CoreV1().ServiceAccounts(ns).Create(ctx, newServiceAccount(ns, saName2), metav1.CreateOptions{})).NotTo(BeNil())
		syncStatus(checkAll)

		clusterRoleHandler.addCallExpected()
		Expect(fakeClient.RbacV1().ClusterRoles().Create(ctx, newClusterRole(crName), metav1.CreateOptions{})).NotTo(BeNil())
		syncStatus(checkAll)

		clusterRoleBindingHandler.addCallExpected()
		saBindingRelation.addCallExpected()
		saBindingRelation.addCallExpected()
		roleBindingRelation.addCallExpected()
		Expect(fakeClient.RbacV1().ClusterRoleBindings().Create(ctx, newClusterRoleBinding(crbName, crName,
			[]types.NamespacedName{{Name: saName, Namespace: ns}, {Name: saName2, Namespace: ns}}),
			metav1.CreateOptions{})).NotTo(BeNil())
		syncStatus(checkAll)

		saBindingRelation.deleteCallExpected()
		saBindingRelation.deleteCallExpected()
		clusterRoleBindingHandler.updateCallExpected()
		Expect(fakeClient.RbacV1().ClusterRoleBindings().Update(ctx, newClusterRoleBinding(crbName, crName,
			[]types.NamespacedName{}),
			metav1.UpdateOptions{})).NotTo(BeNil())
		syncStatus(checkAll)

		saBindingRelation.addCallExpected()
		clusterRoleBindingHandler.updateCallExpected()
		Expect(fakeClient.RbacV1().ClusterRoleBindings().Update(ctx, newClusterRoleBinding(crbName, crName,
			[]types.NamespacedName{{Name: saName, Namespace: ns}}),
			metav1.UpdateOptions{})).NotTo(BeNil())
		syncStatus(checkAll)

		saBindingRelation.addCallExpected()
		saBindingRelation.deleteCallExpected()
		clusterRoleBindingHandler.updateCallExpected()
		Expect(fakeClient.RbacV1().ClusterRoleBindings().Update(ctx, newClusterRoleBinding(crbName, crName,
			[]types.NamespacedName{{Name: saName2, Namespace: ns}}),
			metav1.UpdateOptions{})).NotTo(BeNil())
		syncStatus(checkAll)

		saBindingRelation.addCallExpected()
		saBindingRelation.deleteCallExpected()
		clusterRoleBindingHandler.updateCallExpected()
		Expect(fakeClient.RbacV1().ClusterRoleBindings().Update(ctx, newClusterRoleBinding(crbName, crName,
			[]types.NamespacedName{{Name: saName, Namespace: ns}}),
			metav1.UpdateOptions{})).NotTo(BeNil())
		syncStatus(checkAll)
	})

	It("create two serviceAccounts with clusters and clusterRoleBinding", func() {
		ns := "testclusterresource"
		crbName := "crbtest"
		crName := "crName"
		saName := "saName"
		saName2 := "saName2"
		cluster1 := "cluster1"
		cluster2 := "cluster2"

		saHandler.addCallExpected()
		sa1 := newServiceAccount(ns, saName)
		setObjectCluster(sa1, cluster1)
		Expect(fakeClient.CoreV1().ServiceAccounts(ns).Create(ctx, sa1, metav1.CreateOptions{})).NotTo(BeNil())
		saHandler.addCallExpected()
		sa2 := newServiceAccount(ns, saName2)
		setObjectCluster(sa2, cluster2)
		Expect(fakeClient.CoreV1().ServiceAccounts(ns).Create(ctx, sa2, metav1.CreateOptions{})).NotTo(BeNil())
		syncStatus(checkAll)

		clusterRoleBindingHandler.addCallExpected()
		Expect(fakeClient.RbacV1().ClusterRoleBindings().Create(ctx, newClusterRoleBinding(crbName, crName,
			[]types.NamespacedName{{Name: saName, Namespace: ns}, {Name: saName2, Namespace: ns}}),
			metav1.CreateOptions{})).NotTo(BeNil())
		syncStatus(checkAll)
	})

	It("create two serviceAccounts with clusters and clusterRoleBinding", func() {
		ns := "testclusterresource"
		crbName := "crbtest"
		crName := "crName"
		saName := "saName"
		saName2 := "saName2"
		cluster1 := "cluster1"
		cluster2 := "cluster2"

		saHandler.addCallExpected()
		sa1 := newServiceAccount(ns, saName)
		setObjectCluster(sa1, cluster1)
		Expect(fakeClient.CoreV1().ServiceAccounts(ns).Create(ctx, sa1, metav1.CreateOptions{})).NotTo(BeNil())
		saHandler.addCallExpected()
		sa2 := newServiceAccount(ns, saName2)
		setObjectCluster(sa2, cluster2)
		Expect(fakeClient.CoreV1().ServiceAccounts(ns).Create(ctx, sa2, metav1.CreateOptions{})).NotTo(BeNil())
		syncStatus(checkAll)

		clusterRoleBindingHandler.addCallExpected()
		saBindingRelation.addCallExpected()
		crb := newClusterRoleBinding(crbName, crName,
			[]types.NamespacedName{{Name: saName, Namespace: ns}, {Name: saName2, Namespace: ns}})
		setObjectCluster(crb, cluster1)
		Expect(fakeClient.RbacV1().ClusterRoleBindings().Create(ctx, crb, metav1.CreateOptions{})).NotTo(BeNil())
		syncStatus(checkAll)
	})
	It("create and update clusterRoleBinding with objects not existed", func() {
		ns := "testclusterresource"
		crbName := "crbtest"
		crName := "crName"
		saName := "saName"
		saName2 := "saName2"

		clusterRoleBindingHandler.addCallExpected()
		crb := newClusterRoleBinding(crbName, crName,
			[]types.NamespacedName{{Name: saName, Namespace: ns}})
		Expect(fakeClient.RbacV1().ClusterRoleBindings().Create(ctx, crb, metav1.CreateOptions{})).NotTo(BeNil())
		syncStatus(checkAll)

		clusterRoleBindingHandler.updateCallExpected()
		crb = newClusterRoleBinding(crbName, crName,
			[]types.NamespacedName{{Name: saName, Namespace: ns}, {Name: saName2, Namespace: ns}})
		Expect(fakeClient.RbacV1().ClusterRoleBindings().Update(ctx, crb, metav1.UpdateOptions{})).NotTo(BeNil())
		syncStatus(checkAll)

		clusterRoleBindingHandler.updateCallExpected()
		crb = newClusterRoleBinding(crbName, crName, []types.NamespacedName{{Name: saName2, Namespace: ns}})
		Expect(fakeClient.RbacV1().ClusterRoleBindings().Update(ctx, crb, metav1.UpdateOptions{})).NotTo(BeNil())
		syncStatus(checkAll)

		clusterRoleBindingHandler.updateCallExpected()
		crb = newClusterRoleBinding(crbName, crName, []types.NamespacedName{})
		Expect(fakeClient.RbacV1().ClusterRoleBindings().Update(ctx, crb, metav1.UpdateOptions{})).NotTo(BeNil())
		syncStatus(checkAll)
	})
})

var _ = Describe("test suite with mock relation for fed namespaces and local cluster pods", func() {
	var manager Manager
	var fakeClient *fake.Clientset
	var nsHandler *objecthandler
	var nsPodRelation *relationHandler

	var nsStorage TopoNodeStorage
	var ctx context.Context
	var cancel func()

	checkAll := func() bool {
		return nsHandler.matchExpected() &&
			nsPodRelation.matchExpected()
	}

	BeforeEach(func() {
		fakeClient = fake.NewSimpleClientset()
		k8sInformerFactory := informers.NewSharedInformerFactory(fakeClient, 0)
		var err error
		manager, err = NewResourcesTopoManager(*buildManagerConfig(buildMultiClustertopoConfig(k8sInformerFactory)))
		Expect(err).NotTo(HaveOccurred())

		nsStorage, _ = manager.GetTopoNodeStorage(NamespaceMeta)
		Expect(nsStorage).NotTo(BeNil())

		ctx, cancel = context.WithCancel(context.Background())
		nsHandler = &objecthandler{}
		Expect(manager.AddNodeHandler(NamespaceMeta, nsHandler)).NotTo(HaveOccurred())

		nsPodRelation = &relationHandler{}
		Expect(manager.AddRelationHandler(NamespaceMeta, PodMeta, nsPodRelation)).To(Succeed())

		manager.Start(ctx.Done())
		k8sInformerFactory.Start(ctx.Done())
		k8sInformerFactory.WaitForCacheSync(ctx.Done())
	})

	AfterEach(func() {
		if !checkAll() {
			klog.Infof("end with object [ns %s]", nsHandler.string())
			klog.Infof("end with relation [nsPod %s]", nsPodRelation.string())
		}
		cancel()
	})

	It("create fed ns and local pod ", func() {
		nsName := "ns"
		podName := "podName"
		fedCluster := "fed"
		localCluster := "localCluster"

		ns1 := newNamespaceWithCluster(nsName, fedCluster)
		pod := newPod(nsName, podName)
		setObjectCluster(pod, localCluster)
		setMultiClusterDepend(ns1, []MultiClusterDepend{{
			Cluster:   localCluster,
			Namespace: nsName,
			Name:      podName,
		}})

		nsHandler.addCallExpected()
		Expect(fakeClient.CoreV1().Namespaces().Create(ctx, ns1, metav1.CreateOptions{})).NotTo(BeNil())
		syncStatus(checkAll)

		nsHandler.relatedCallExpected()
		nsPodRelation.addCallExpected()
		Expect(fakeClient.CoreV1().Pods(nsName).Create(ctx, pod, metav1.CreateOptions{})).NotTo(BeNil())
		syncStatus(checkAll)
	})

	It("create fed ns and local pod in different clusters, test pod recreation", func() {
		nsName := "ns"
		podName := "podName"
		podName2 := "podName2"
		fedCluster := "fed"
		localCluster := "localCluster"
		localCluster2 := "localCluster2"

		ns1 := newNamespaceWithCluster(nsName, fedCluster)
		pod := newPod(nsName, podName)
		setObjectCluster(pod, localCluster)
		pod2 := newPod(nsName, podName2)
		setObjectCluster(pod2, localCluster2)
		setMultiClusterDepend(ns1, []MultiClusterDepend{
			{
				Cluster:   localCluster,
				Namespace: nsName,
				Name:      podName,
			},
			{
				Cluster:   localCluster2,
				Namespace: nsName,
				Name:      podName2,
			},
		})

		nsHandler.addCallExpected()
		Expect(fakeClient.CoreV1().Namespaces().Create(ctx, ns1, metav1.CreateOptions{})).NotTo(BeNil())
		syncStatus(checkAll)

		nsHandler.relatedCallExpected()
		nsPodRelation.addCallExpected()
		Expect(fakeClient.CoreV1().Pods(nsName).Create(ctx, pod, metav1.CreateOptions{})).NotTo(BeNil())
		syncStatus(checkAll)

		nsHandler.relatedCallExpected()
		nsPodRelation.deleteCallExpected()
		Expect(fakeClient.CoreV1().Pods(nsName).Delete(ctx, podName, metav1.DeleteOptions{})).To(Succeed())
		syncStatus(checkAll)

		nsHandler.relatedCallExpected()
		nsPodRelation.addCallExpected()
		Expect(fakeClient.CoreV1().Pods(nsName).Create(ctx, pod2, metav1.CreateOptions{})).NotTo(BeNil())
		syncStatus(checkAll)

		nsHandler.relatedCallExpected()
		nsPodRelation.addCallExpected()
		Expect(fakeClient.CoreV1().Pods(nsName).Create(ctx, pod, metav1.CreateOptions{})).NotTo(BeNil())
		syncStatus(checkAll)
	})

	It("create ns and pod in different cluster, test relation config changed", func() {
		nsName := "ns"
		podName := "podName"
		podName2 := "podName2"
		fedCluster := "fed"
		localCluster := "localCluster"
		localCluster2 := "localCluster2"

		ns1 := newNamespaceWithCluster(nsName, fedCluster)
		pod := newPod(nsName, podName)
		setObjectCluster(pod, localCluster)
		pod2 := newPod(nsName, podName2)
		setObjectCluster(pod2, localCluster2)
		setMultiClusterDepend(ns1, []MultiClusterDepend{
			{
				Cluster:   localCluster,
				Namespace: nsName,
				Name:      podName,
			},
			{
				Cluster:   localCluster2,
				Namespace: nsName,
				Name:      podName2,
			},
		})

		nsHandler.addCallExpected()
		Expect(fakeClient.CoreV1().Namespaces().Create(ctx, ns1, metav1.CreateOptions{})).NotTo(BeNil())
		syncStatus(checkAll)

		nsHandler.relatedCallExpected()
		nsPodRelation.addCallExpected()
		Expect(fakeClient.CoreV1().Pods(nsName).Create(ctx, pod, metav1.CreateOptions{})).NotTo(BeNil())
		syncStatus(checkAll)

		nsHandler.relatedCallExpected()
		nsPodRelation.addCallExpected()
		Expect(fakeClient.CoreV1().Pods(nsName).Create(ctx, pod2, metav1.CreateOptions{})).NotTo(BeNil())
		syncStatus(checkAll)

		setMultiClusterDepend(ns1, []MultiClusterDepend{})
		nsHandler.updateCallExpected()
		nsPodRelation.deleteCallExpected()
		nsPodRelation.deleteCallExpected()
		Expect(fakeClient.CoreV1().Namespaces().Update(ctx, ns1, metav1.UpdateOptions{})).NotTo(BeNil())
		syncStatus(checkAll)

		setMultiClusterDepend(ns1, []MultiClusterDepend{{
			Cluster:   localCluster,
			Namespace: nsName,
			Name:      podName,
		}})
		nsHandler.updateCallExpected()
		nsPodRelation.addCallExpected()
		Expect(fakeClient.CoreV1().Namespaces().Update(ctx, ns1, metav1.UpdateOptions{})).NotTo(BeNil())
		syncStatus(checkAll)

		nsHandler.relatedCallExpected()
		nsPodRelation.deleteCallExpected()
		Expect(fakeClient.CoreV1().Pods(nsName).Delete(ctx, pod.Name, metav1.DeleteOptions{})).To(Succeed())
		syncStatus(checkAll)

		nsHandler.relatedCallExpected()
		nsPodRelation.addCallExpected()
		Expect(fakeClient.CoreV1().Pods(nsName).Create(ctx, pod, metav1.CreateOptions{})).NotTo(BeNil())
		syncStatus(checkAll)

		setMultiClusterDepend(ns1, []MultiClusterDepend{{
			Cluster:   localCluster2,
			Namespace: nsName,
			Name:      podName2,
		}})
		nsHandler.updateCallExpected()
		nsPodRelation.deleteCallExpected()
		nsPodRelation.addCallExpected()
		Expect(fakeClient.CoreV1().Namespaces().Update(ctx, ns1, metav1.UpdateOptions{})).NotTo(BeNil())
		syncStatus(checkAll)
	})
})

var _ = Describe("test suite for multi routine", func() {
	var manager Manager
	var fakeClient *fake.Clientset
	var podHandler, replicasetHandler, deployHandler, inspectDeployHandler *objecthandler
	var podRsRelation, rsDeployRelation *relationHandler

	var podStorage, replicasetStorage, deployStorage, inspectDeployStorage TopoNodeStorage
	var ctx context.Context
	var cancel func()
	BeforeEach(func() {
		fakeClient = fake.NewSimpleClientset()
		k8sInformerFactory := informers.NewSharedInformerFactory(fakeClient, 0)
		var err error
		manager, err = NewResourcesTopoManager(*buildManagerConfig(buildDeployTopoConfig(k8sInformerFactory)))
		Expect(err).NotTo(HaveOccurred())

		replicasetStorage, _ = manager.GetTopoNodeStorage(ReplicaSetMeta)
		podStorage, _ = manager.GetTopoNodeStorage(PodMeta)
		deployStorage, _ = manager.GetTopoNodeStorage(DeployMeta)
		inspectDeployStorage, _ = manager.GetTopoNodeStorage(InspectDeployMeta)
		Expect(replicasetStorage).NotTo(BeNil())
		Expect(podStorage).NotTo(BeNil())
		Expect(deployStorage).NotTo(BeNil())
		Expect(inspectDeployStorage).NotTo(BeNil())

		ctx, cancel = context.WithCancel(context.Background())
		podHandler = &objecthandler{needRangePostOrder: true, needRangePreOrder: true}
		Expect(manager.AddNodeHandler(PodMeta, podHandler)).Should(Succeed())
		replicasetHandler = &objecthandler{needRangePostOrder: true, needRangePreOrder: true}
		Expect(manager.AddNodeHandler(ReplicaSetMeta, replicasetHandler)).Should(Succeed())
		deployHandler = &objecthandler{needRangePostOrder: true, needRangePreOrder: true}
		Expect(manager.AddNodeHandler(DeployMeta, deployHandler)).Should(Succeed())
		inspectDeployHandler = &objecthandler{needRangePostOrder: true, needRangePreOrder: true}
		Expect(manager.AddNodeHandler(InspectDeployMeta, inspectDeployHandler)).Should(Succeed())

		podRsRelation = &relationHandler{}
		Expect(manager.AddRelationHandler(ReplicaSetMeta, PodMeta, podRsRelation)).Should(Succeed())
		rsDeployRelation = &relationHandler{}
		Expect(manager.AddRelationHandler(DeployMeta, ReplicaSetMeta, rsDeployRelation)).Should(Succeed())

		manager.Start(ctx.Done())
		k8sInformerFactory.Start(ctx.Done())
		k8sInformerFactory.WaitForCacheSync(ctx.Done())
	})
	AfterEach(func() {
		klog.V(loglevel).Infof("end with object [rs %s, pod %s, deploy %s, inspectDeploy %s]", replicasetHandler.string(), podHandler.string(), deployHandler.string(), inspectDeployHandler.string())
		klog.V(loglevel).Infof("end with relation [rsDeploy %s, podRs %s]", rsDeployRelation.string(), podRsRelation.string())
		cancel()
	})

	It("test multi routine with pod creation, update and deletion", func() {
		deployNamePrefix := "testDeploy"
		rsNamePrefix := deployNamePrefix + "-rs"
		rsNames := []string{rsNamePrefix + "1", rsNamePrefix + "2"}
		podNamePrefix := rsNamePrefix + "-pod"
		appsKey := "apps"

		labels := []string{appsKey, deployNamePrefix}
		Expect(fakeClient.AppsV1().Deployments(namespaceDefault).Create(ctx, newDeploy(namespaceDefault, deployNamePrefix, labels...), metav1.CreateOptions{})).NotTo(BeNil())
		for _, rsName := range rsNames {
			rs := newReplicaSet(namespaceDefault, rsName, labels...)
			setOwner(rs, DeployMeta, deployNamePrefix)
			Expect(fakeClient.AppsV1().ReplicaSets(namespaceDefault).Create(ctx, rs, metav1.CreateOptions{})).NotTo(BeNil())
		}

		totalNum := int(maxChanSize)
		for i := 0; i < totalNum; i++ {
			podName := fmt.Sprintf("%s-%d", podNamePrefix, i)
			pod := newPod(namespaceDefault, podName, labels...)
			setOwner(pod, ReplicaSetMeta, rsNames[i%len(rsNames)])
			klog.V(loglevel).Infof("add pod %s", podName)
			go func() {
				fakeClient.CoreV1().Pods(namespaceDefault).Create(ctx, pod, metav1.CreateOptions{})
			}()
		}

		notMatchNum := make([]int, len(rsNames))
		var lock sync.Mutex
		notMatchCount := func(i int) {
			lock.Lock()
			defer lock.Unlock()
			notMatchNum[i] += 1
		}

		updateLabel2NotMatch := func(podName string, succssCall func()) {
			pod, _ := fakeClient.CoreV1().Pods(namespaceDefault).Get(ctx, podName, metav1.GetOptions{})
			if pod != nil {
				pod.Labels[appsKey] = "test2"
				if _, err := fakeClient.CoreV1().Pods(namespaceDefault).Update(ctx, pod, metav1.UpdateOptions{}); err == nil {
					klog.V(loglevel).Infof("update pod %s labels to no longer match", podName)
					succssCall()
				}
			}
		}

		updateLabel2StillMatch := func(podName string, succssCall func()) {
			pod, _ := fakeClient.CoreV1().Pods(namespaceDefault).Get(ctx, podName, metav1.GetOptions{})
			if pod != nil {
				pod.Labels["apps2"] = deployNamePrefix
				if _, err := fakeClient.CoreV1().Pods(namespaceDefault).Update(ctx, pod, metav1.UpdateOptions{}); err == nil {
					klog.V(loglevel).Infof("update pod %s labels", podName)
					succssCall()
				}
			}
		}

		deletePod := func(podName string, succssCall func()) {
			pod, _ := fakeClient.CoreV1().Pods(namespaceDefault).Get(ctx, podName, metav1.GetOptions{})
			if pod != nil {
				if err := fakeClient.CoreV1().Pods(namespaceDefault).Delete(ctx, pod.Name, metav1.DeleteOptions{}); err == nil {
					klog.V(loglevel).Infof("delete pod %s", podName)
					succssCall()
				}
			}
		}

		for i := 0; i < totalNum; i++ {
			podName := fmt.Sprintf("%s-%d", podNamePrefix, i)
			switch i % 3 {
			case 0:
				go updateLabel2StillMatch(podName, func() {})
			case 1:
				go deletePod(podName, func() { notMatchCount(i % len(rsNames)) })
			case 2:
				go updateLabel2NotMatch(podName, func() { notMatchCount(i % len(rsNames)) })
			}
		}

		Eventually(func(g Gomega) {
			deploy, _ := deployStorage.GetNode(types.NamespacedName{Name: deployNamePrefix, Namespace: namespaceDefault})

			g.Expect(deploy).NotTo(BeNil())
			g.Expect(deploy.GetPreOrders()).To(HaveLen(1))
			postOrders := deploy.GetPostOrders()
			g.Expect(postOrders).To(HaveLen(len(rsNames)))
			for i := 0; i < len(rsNames); i++ {
				rsNode, _ := replicasetStorage.GetNode(types.NamespacedName{Name: rsNames[i], Namespace: namespaceDefault})

				g.Expect(rsNode).NotTo(BeNil())
				g.Expect(rsNode.GetPreOrders()).To(HaveLen(1))
				// nolint:ginkgolinter
				g.Expect(len(rsNode.GetPostOrders())).To(Equal(totalNum/len(rsNames) - notMatchNum[i]))
			}
		}, "10s").Should(Succeed())
	})

	It("test multi routine with pod and rs modification", func() {
		deployNamePrefix := "testDeploy"
		rsNamePrefix := deployNamePrefix + "-rs"
		rsNum := 10
		rsNames := make([]string, rsNum)
		for i := 0; i < rsNum; i++ {
			rsNames[i] = fmt.Sprintf("%s-%d", rsNamePrefix, i)
		}
		podNamePrefix := rsNamePrefix + "-pod"
		appsKey := "apps"

		labels := []string{appsKey, deployNamePrefix}
		Expect(fakeClient.AppsV1().Deployments(namespaceDefault).Create(ctx, newDeploy(namespaceDefault, deployNamePrefix, labels...), metav1.CreateOptions{})).NotTo(BeNil())

		totalNum := rsNum * 1000
		for i := 0; i < totalNum; i++ {
			podName := fmt.Sprintf("%s-%d", podNamePrefix, i)
			pod := newPod(namespaceDefault, podName, labels...)
			setOwner(pod, ReplicaSetMeta, rsNames[i%len(rsNames)])
			klog.V(loglevel).Infof("add pod %s", podName)
			fakeClient.CoreV1().Pods(namespaceDefault).Create(ctx, pod, metav1.CreateOptions{})
		}

		updateLabel2StillMatch := func(podName string, succssCall func()) {
			pod, _ := fakeClient.CoreV1().Pods(namespaceDefault).Get(ctx, podName, metav1.GetOptions{})
			if pod != nil {
				pod.Labels["apps2"] = deployNamePrefix
				if _, err := fakeClient.CoreV1().Pods(namespaceDefault).Update(ctx, pod, metav1.UpdateOptions{}); err == nil {
					klog.V(loglevel).Infof("update pod %s labels", podName)
					succssCall()
				}
			}
		}

		for _, rsName := range rsNames {
			rs := newReplicaSet(namespaceDefault, rsName, labels...)
			setOwner(rs, DeployMeta, deployNamePrefix)
			Expect(fakeClient.AppsV1().ReplicaSets(namespaceDefault).Create(ctx, rs, metav1.CreateOptions{})).NotTo(BeNil())
		}

		loopwrapper := func(f func()) {
			for i := 0; i < 50; i++ {
				f()
			}
		}
		for i, rsName := range rsNames {
			rs := newReplicaSet(namespaceDefault, rsName, labels...)
			setOwner(rs, DeployMeta, deployNamePrefix)

			switch i % 3 {
			case 0:
				go loopwrapper(func() {
					Expect(fakeClient.AppsV1().ReplicaSets(namespaceDefault).Delete(ctx, rsName, metav1.DeleteOptions{})).To(Succeed())
					Expect(fakeClient.AppsV1().ReplicaSets(namespaceDefault).Create(ctx, rs, metav1.CreateOptions{})).NotTo(BeNil())
				})
			case 1:
				go loopwrapper(func() {
					rs.Labels[appsKey] = "notExistDeploy"
					Expect(fakeClient.AppsV1().ReplicaSets(namespaceDefault).Update(ctx, rs, metav1.UpdateOptions{})).NotTo(BeNil())
					rs.Labels[appsKey] = deployNamePrefix
					Expect(fakeClient.AppsV1().ReplicaSets(namespaceDefault).Update(ctx, rs, metav1.UpdateOptions{})).NotTo(BeNil())
				})
			case 2:
				go loopwrapper(func() {
					rs.OwnerReferences = nil
					Expect(fakeClient.AppsV1().ReplicaSets(namespaceDefault).Update(ctx, rs, metav1.UpdateOptions{})).NotTo(BeNil())
					setOwner(rs, DeployMeta, deployNamePrefix)
					Expect(fakeClient.AppsV1().ReplicaSets(namespaceDefault).Update(ctx, rs, metav1.UpdateOptions{})).NotTo(BeNil())
				})
			}
		}

		for i := 0; i < totalNum; i++ {
			podName := fmt.Sprintf("%s-%d", podNamePrefix, i)
			go updateLabel2StillMatch(podName, func() {})
		}

		Eventually(func(g Gomega) {
			deploy, _ := deployStorage.GetNode(types.NamespacedName{Name: deployNamePrefix, Namespace: namespaceDefault})
			g.Expect(deploy).NotTo(BeNil())
			g.Expect(deploy.GetPreOrders()).To(HaveLen(1))
			postOrders := deploy.GetPostOrders()
			// nolint:ginkgolinter
			g.Expect(len(postOrders)).To(Equal(rsNum))
			for i := 0; i < len(rsNames); i++ {
				rsNode, _ := replicasetStorage.GetNode(types.NamespacedName{Name: rsNames[i], Namespace: namespaceDefault})
				g.Expect(rsNode).NotTo(BeNil())
				g.Expect(rsNode.GetPreOrders()).To(HaveLen(1))
				// nolint:ginkgolinter
				g.Expect(len(rsNode.GetPostOrders())).To(Equal(totalNum / len(rsNames)))
			}
		}, "10s").Should(Succeed())
	})
})
