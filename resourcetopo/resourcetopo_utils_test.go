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
	"encoding/json"
	"fmt"

	. "github.com/onsi/ginkgo"

	"github.com/hashicorp/consul/sdk/testutil/retry"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	"k8s.io/klog/v2"

	"kusionstack.io/kube-utils/multicluster/clusterinfo"
)

const namespaceDefault = "default"

var (
	PodMeta                = metav1.TypeMeta{Kind: "Pod", APIVersion: "core/v1"}
	ServiceMeta            = metav1.TypeMeta{Kind: "Service", APIVersion: "core/v1"}
	ReplicaSetMeta         = metav1.TypeMeta{Kind: "ReplicaSet", APIVersion: "apps/v1"}
	DeployMeta             = metav1.TypeMeta{Kind: "Deployment", APIVersion: "apps/v1"}
	StatefulSetMeta        = metav1.TypeMeta{Kind: "StatefulSet", APIVersion: "apps/v1"}
	InspectStatefulSetMeta = metav1.TypeMeta{Kind: "StatefulSet", APIVersion: "inspect/v1"}
	ClusterRoleBindingMeta = metav1.TypeMeta{Kind: "ClusterRoleBinding", APIVersion: "rbac/v1"}
	ClusterRoleMeta        = metav1.TypeMeta{Kind: "ClusterRole", APIVersion: "rbac/v1"}
	ServiceAccountMeta     = metav1.TypeMeta{Kind: "ServiceAccount", APIVersion: "core/v1"}
	NamespaceMeta          = metav1.TypeMeta{Kind: "Namespace", APIVersion: "core/v1"}
)

func GetInformer(meta metav1.TypeMeta, k8sInformerFactory informers.SharedInformerFactory) Informer {
	gvk := meta.String()
	switch gvk {
	case PodMeta.String():
		return k8sInformerFactory.Core().V1().Pods().Informer()
	case ServiceMeta.String():
		return k8sInformerFactory.Core().V1().Services().Informer()
	case StatefulSetMeta.String():
		return k8sInformerFactory.Apps().V1().StatefulSets().Informer()
	case ClusterRoleBindingMeta.String():
		return k8sInformerFactory.Rbac().V1().ClusterRoleBindings().Informer()
	case ClusterRoleMeta.String():
		return k8sInformerFactory.Rbac().V1().ClusterRoles().Informer()
	case ServiceAccountMeta.String():
		return k8sInformerFactory.Core().V1().ServiceAccounts().Informer()
	case DeployMeta.String():
		return k8sInformerFactory.Apps().V1().Deployments().Informer()
	case ReplicaSetMeta.String():
		return k8sInformerFactory.Apps().V1().ReplicaSets().Informer()
	case NamespaceMeta.String():
		return k8sInformerFactory.Core().V1().Namespaces().Informer()

	default:
		return nil
	}
}

func buildManagerConfig(config *TopologyConfig) *ManagerConfig {
	return &ManagerConfig{
		TopologyConfig: config,
	}
}

func buildInspectTopoConfig(k8sInformerFactory informers.SharedInformerFactory) *TopologyConfig {
	return &TopologyConfig{
		GetInformer: func(meta metav1.TypeMeta) Informer {
			return GetInformer(meta, k8sInformerFactory)
		},
		Resolvers: []RelationResolver{
			{
				PreMeta:   StatefulSetMeta,
				PostMetas: []metav1.TypeMeta{PodMeta},
				Resolve: func(preOrder Object) []ResourceRelation {
					stsObj, ok := preOrder.(*appsv1.StatefulSet)
					if !ok {
						return nil
					}
					labelSelector := stsObj.Spec.Selector
					return []ResourceRelation{
						{
							PostMeta:      PodMeta,
							LabelSelector: labelSelector,
						},
					}
				},
			},
		},
		Discoverers: []VirtualResourceDiscoverer{
			{
				PreMeta:  InspectStatefulSetMeta,
				PostMeta: StatefulSetMeta,
				Discover: func(postObject Object) []types.NamespacedName {
					sts := postObject.(*appsv1.StatefulSet)

					return []types.NamespacedName{
						{
							Name:      sts.Name,
							Namespace: sts.Namespace,
						},
					}
				},
			},
		},
	}
}

func buildClusterTest(k8sInformerFactory informers.SharedInformerFactory) *TopologyConfig {
	return &TopologyConfig{
		GetInformer: func(meta metav1.TypeMeta) Informer {
			return GetInformer(meta, k8sInformerFactory)
		},
		Resolvers: []RelationResolver{
			{
				PreMeta:   ClusterRoleBindingMeta,
				PostMetas: []metav1.TypeMeta{ClusterRoleMeta, PodMeta, ServiceAccountMeta},
				Resolve: func(preOrder Object) []ResourceRelation {
					crbObject, ok := preOrder.(*rbacv1.ClusterRoleBinding)
					if !ok {
						return nil
					}
					var saRefs []types.NamespacedName
					cluster := getObjectCluster(crbObject)

					for _, s := range crbObject.Subjects {
						switch s.Kind {
						case ServiceAccountMeta.Kind:
							saRefs = append(saRefs, types.NamespacedName{Name: s.Name, Namespace: s.Namespace})
						}
					}
					return []ResourceRelation{
						{
							PostMeta: ClusterRoleMeta,
							Cluster:  cluster,
							DirectRefs: []types.NamespacedName{
								{
									Name: crbObject.RoleRef.Name,
								},
							},
						},
						{
							PostMeta:   ServiceAccountMeta,
							Cluster:    cluster,
							DirectRefs: saRefs,
						},
					}
				},
			},
		},
	}
}

func buildSvcPodTest(k8sInformerFactory informers.SharedInformerFactory) *TopologyConfig {
	return &TopologyConfig{
		GetInformer: func(meta metav1.TypeMeta) Informer {
			return GetInformer(meta, k8sInformerFactory)
		},
		Resolvers: []RelationResolver{
			{
				PreMeta:       ServiceMeta,
				PostMetas:     []metav1.TypeMeta{PodMeta},
				ReverseNotice: []metav1.TypeMeta{PodMeta},
				Resolve: func(preOrder Object) []ResourceRelation {
					svcObject, ok := preOrder.(*corev1.Service)
					if !ok {
						return nil
					}
					label := (svcObject.Spec.Selector)
					return []ResourceRelation{
						{
							PostMeta: PodMeta,
							LabelSelector: &metav1.LabelSelector{
								MatchLabels: label,
							},
						},
					}
				},
			},
		},
	}
}

func buildDeployTopoConfig(k8sInformerFactory informers.SharedInformerFactory) *TopologyConfig {
	return &TopologyConfig{
		GetInformer: func(meta metav1.TypeMeta) Informer {
			return GetInformer(meta, k8sInformerFactory)
		},
		Resolvers: []RelationResolver{
			{
				PreMeta:       DeployMeta,
				PostMetas:     []metav1.TypeMeta{ReplicaSetMeta},
				OwnerRelation: []metav1.TypeMeta{ReplicaSetMeta},
				Resolve: func(preOrder Object) []ResourceRelation {
					deployObj, ok := preOrder.(*appsv1.Deployment)
					if !ok {
						return nil
					}
					labelSelector := deployObj.Spec.Selector
					return []ResourceRelation{
						{
							PostMeta:      ReplicaSetMeta,
							LabelSelector: labelSelector,
						},
					}
				},
			},
			{
				PreMeta:       ReplicaSetMeta,
				PostMetas:     []metav1.TypeMeta{PodMeta},
				OwnerRelation: []metav1.TypeMeta{PodMeta},
				Resolve: func(preOrder Object) []ResourceRelation {
					rsObj, ok := preOrder.(*appsv1.ReplicaSet)
					if !ok {
						return nil
					}
					labelSelector := rsObj.Spec.Selector
					return []ResourceRelation{
						{
							PostMeta:      PodMeta,
							LabelSelector: labelSelector,
						},
					}
				},
			},
		},
	}
}

func buildMultiClustertopoConfig(k8sInformerFactory informers.SharedInformerFactory) *TopologyConfig {
	return &TopologyConfig{
		GetInformer: func(meta metav1.TypeMeta) Informer {
			return GetInformer(meta, k8sInformerFactory)
		},
		Resolvers: []RelationResolver{
			{
				PreMeta:   NamespaceMeta,
				PostMetas: []metav1.TypeMeta{PodMeta},
				Resolve: func(preOrder Object) []ResourceRelation {
					preObj, ok := preOrder.(*corev1.Namespace)
					if !ok {
						return nil
					}
					depends := getMultiClusterDepend(&preObj.ObjectMeta)
					var relations []ResourceRelation
					for _, v := range depends {
						relations = append(relations, ResourceRelation{
							PostMeta: PodMeta,
							Cluster:  v.Cluster,
							DirectRefs: []types.NamespacedName{{
								Name:      v.Name,
								Namespace: v.Namespace,
							}},
						})
					}

					return relations
				},
			},
		},
	}
}

func newPod(namespace, name string, labels ...string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: *newObjectMeta(namespace, name, labels),
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  name,
					Image: "busybox",
				},
			},
		},
	}
}

func newStatefulSet(namespace, name string, labels ...string) *appsv1.StatefulSet {
	sts := &appsv1.StatefulSet{
		ObjectMeta: *newObjectMeta(namespace, name, labels),
		Spec: appsv1.StatefulSetSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  name,
							Image: "busybox",
						},
					},
				},
			},
		},
	}
	sts.Spec.Selector = &metav1.LabelSelector{
		MatchLabels: sts.ObjectMeta.Labels,
	}
	return sts
}

func newSvc(namespace, name string, labels ...string) *corev1.Service {
	svc := &corev1.Service{
		ObjectMeta: *newObjectMeta(namespace, name, labels),
		Spec:       corev1.ServiceSpec{},
	}
	svc.Spec.Selector = svc.Labels
	return svc
}

func newDeploy(namespace, name string, labels ...string) *appsv1.Deployment {
	deploy := &appsv1.Deployment{
		ObjectMeta: *newObjectMeta(namespace, name, labels),
		Spec: appsv1.DeploymentSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  name,
							Image: "busybox",
						},
					},
				},
			},
		},
	}
	deploy.Spec.Selector = &metav1.LabelSelector{
		MatchLabels: deploy.ObjectMeta.Labels,
	}
	return deploy
}

func newReplicaSet(namespace, name string, labels ...string) *appsv1.ReplicaSet {
	replicaset := &appsv1.ReplicaSet{
		ObjectMeta: *newObjectMeta(namespace, name, labels),
		Spec: appsv1.ReplicaSetSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  name,
							Image: "busybox",
						},
					},
				},
			},
		},
	}
	replicaset.Spec.Selector = &metav1.LabelSelector{
		MatchLabels: replicaset.ObjectMeta.Labels,
	}
	return replicaset
}

func newClusterRole(name string) *rbacv1.ClusterRole {
	return &rbacv1.ClusterRole{
		ObjectMeta: *newObjectMeta("", name, nil),
	}
}

func newClusterRoleBinding(name string, clusterRole string, sas []types.NamespacedName) *rbacv1.ClusterRoleBinding {
	crb := &rbacv1.ClusterRoleBinding{
		ObjectMeta: *newObjectMeta("", name, nil),
		RoleRef: rbacv1.RoleRef{
			APIGroup: ClusterRoleMeta.APIVersion,
			Kind:     ClusterRoleMeta.Kind,
			Name:     clusterRole,
		},
	}
	for _, sa := range sas {
		crb.Subjects = append(crb.Subjects, rbacv1.Subject{
			Kind:      ServiceAccountMeta.Kind,
			Name:      sa.Name,
			Namespace: sa.Namespace,
		})
	}
	return crb
}

func newServiceAccount(namespace, name string, labels ...string) *corev1.ServiceAccount {
	return &corev1.ServiceAccount{
		ObjectMeta: *newObjectMeta(namespace, name, labels),
	}
}

func newNamespaceWithCluster(name string, cluster string) *corev1.Namespace {
	ns := &corev1.Namespace{
		ObjectMeta: *newObjectMeta("", name, nil),
	}
	setObjectCluster(ns, cluster)
	return ns
}

type MultiClusterDepend struct {
	Cluster   string `json:"cluster"`
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
}

func syncStatus(f func() bool) {
	retry.RunWith(retry.TwoSeconds(), GinkgoT(), func(r *retry.R) {
		if !f() {
			r.Fatal()
		}
	})
}

func newObjectMeta(namespace, name string, labels []string) *metav1.ObjectMeta {
	labelMap := make(map[string]string)
	for i := 0; i < len(labels)-1; i += 2 {
		labelMap[labels[i]] = labels[i+1]
	}
	return &metav1.ObjectMeta{
		Name:      name,
		Namespace: namespace,
		Labels:    labelMap,
	}
}

func setOwner(object metav1.Object, meta metav1.TypeMeta, ownerName string) {
	object.SetOwnerReferences(append(object.GetOwnerReferences(), metav1.OwnerReference{
		APIVersion: meta.APIVersion,
		Kind:       meta.Kind,
		Name:       ownerName,
	}))
}

func setObjectCluster(obj Object, cluster string) {
	if labels := obj.GetLabels(); labels != nil {
		labels[clusterinfo.ClusterLabelKey] = cluster
	} else {
		panic("labels is nil")
	}
}

const multiClusterDependKey = "kusionstack.io/depends-on"

func setMultiClusterDepend(object metav1.Object, depends []MultiClusterDepend) {
	if info, err := json.Marshal(depends); err != nil {
		panic(err)
	} else {
		anno := object.GetAnnotations()
		if anno == nil {
			anno = make(map[string]string)
			object.SetAnnotations(anno)
		}
		anno[multiClusterDependKey] = string(info)
	}
}

func getMultiClusterDepend(object metav1.Object) []MultiClusterDepend {
	if len(object.GetAnnotations()) == 0 {
		return nil
	}
	if info, ok := object.GetAnnotations()[multiClusterDependKey]; ok {
		var depends []MultiClusterDepend
		if err := json.Unmarshal([]byte(info), &depends); err != nil {
			panic(err)
		}
		return depends
	} else {
		return nil
	}
}

var _ NodeHandler = &objecthandler{}

type objecthandler struct {
	addCounter     int
	updateCounter  int
	relatedCounter int
	deletedCounter int
}

// change loglevel flag to 0 to enable log output
const loglevel = 1

func (o *objecthandler) OnAdd(info NodeInfo) {
	klog.V(loglevel).Infof("received added object %v %v", info.TypeInfo(), info.NodeInfo())
	o.addCounter--
}

func (o *objecthandler) OnUpdate(info NodeInfo) {
	klog.V(loglevel).Infof("received updated object %v %v", info.TypeInfo(), info.NodeInfo())
	o.updateCounter--
}

func (o *objecthandler) OnDelete(info NodeInfo) {
	klog.V(loglevel).Infof("received deleted object %v %v", info.TypeInfo(), info.NodeInfo())
	o.deletedCounter--
}

func (o *objecthandler) OnRelatedUpdate(info NodeInfo) {
	klog.V(loglevel).Infof("received related updated object %v %v", info.TypeInfo(), info.NodeInfo())
	o.relatedCounter--
}

func (o *objecthandler) addCallExpected() *objecthandler {
	o.addCounter++
	return o
}

func (o *objecthandler) updateCallExpected() *objecthandler {
	o.updateCounter++
	return o
}

func (o *objecthandler) deleteCallExpected() *objecthandler {
	o.deletedCounter++
	return o
}

func (o *objecthandler) relatedCallExpected() *objecthandler {
	o.relatedCounter++
	return o
}

func (h *objecthandler) matchExpected() bool {
	return h.addCounter == 0 && h.updateCounter == 0 && h.deletedCounter == 0 && h.relatedCounter == 0
}

func (o *objecthandler) string() string {
	return fmt.Sprintf("{add: %d, update: %d, delete %d, relatedUpdate %d}", o.addCounter, o.updateCounter, o.deletedCounter, o.relatedCounter)
}

var _ RelationHandler = &relationHandler{}

type relationHandler struct {
	addCounter    int
	deleteCounter int
}

func (r *relationHandler) OnAdd(preNode NodeInfo, postNode NodeInfo) {
	klog.V(loglevel).Infof("received added relation, preNode %v %v, postNode %v %v",
		preNode.TypeInfo(), preNode.NodeInfo(), postNode.TypeInfo(), postNode.NodeInfo())
	r.addCounter--
}

func (r *relationHandler) OnDelete(preNode NodeInfo, postNode NodeInfo) {
	klog.V(loglevel).Infof("received deleted relation, preNode %v %v, postNode %v %v",
		preNode.TypeInfo(), preNode.NodeInfo(), postNode.TypeInfo(), postNode.NodeInfo())
	r.deleteCounter--
}

func (r *relationHandler) addCallExpected() *relationHandler {
	r.addCounter++
	return r
}

func (r *relationHandler) deleteCallExpected() *relationHandler {
	r.deleteCounter++
	return r
}

func (h *relationHandler) matchExpected() bool {
	return h.addCounter == 0 && h.deleteCounter == 0
}

func (o *relationHandler) string() string {
	return fmt.Sprintf("{add: %d, delete %d}", o.addCounter, o.deleteCounter)
}
