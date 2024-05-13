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

package main

import (
	"context"
	"fmt"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/manager/signals"

	"kusionstack.io/kube-utils/resourcetopo"
)

var (
	mgrCache      cache.Cache
	backGroundCtx context.Context
)

func main() {
	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{})
	if err != nil {
		klog.Fatal(err.Error())
	}
	mgrCache = mgr.GetCache()
	backGroundCtx = signals.SetupSignalHandler()

	topoManager, err := resourcetopo.NewResourcesTopoManager(
		resourcetopo.ManagerConfig{
			NodeEventQueueSize:     1024,
			RelationEventQueueSize: 1024,
			TopologyConfig:         buildExampleTopologyConfig(), // could also be set later by topoManager.AddTopologyConfig
		},
	)
	if err != nil {
		klog.Fatal(err.Error())
	}

	// AddTopologyConfig could be called multiple times for different relations
	if err = topoManager.AddTopologyConfig(*buildVirtualSymptomTopology()); err != nil {
		klog.Fatal(err.Error())
	}

	if err = topoManager.AddNodeHandler(podMeta, &podEventhandler{}); err != nil {
		klog.Fatal(err.Error())
	}
	if err = topoManager.AddRelationHandler(virtualSymptomMeta, podMeta, &symptomPodRelationEventHandler{}); err != nil {
		klog.Fatal(err.Error())
	}

	topoManager.Start(backGroundCtx.Done())
	if err = mgr.Start(backGroundCtx); err != nil {
		klog.Fatal(err.Error())
	}
}

var (
	deploymentMeta = metav1.TypeMeta{
		Kind:       "Deployment",
		APIVersion: "apps/v1",
	}
	podMeta = metav1.TypeMeta{
		Kind:       "Pod",
		APIVersion: "core/v1",
	}
	persistentVolumeClaimMeta = metav1.TypeMeta{
		Kind:       "PersistentVolumeClaim",
		APIVersion: "core/v1",
	}
	serviceMeta = metav1.TypeMeta{
		Kind:       "Service",
		APIVersion: "core/v1",
	}
	virtualSymptomMeta = metav1.TypeMeta{
		Kind:       "Symptom",
		APIVersion: "inspect.kusionstack.io/v1alpha1",
	}
)

func buildExampleTopologyConfig() *resourcetopo.TopologyConfig {
	return &resourcetopo.TopologyConfig{
		GetInformer: getInformer,
		Resolvers: []resourcetopo.RelationResolver{
			{
				// this block define the relation between deployment and pod
				PreMeta:   deploymentMeta,
				PostMetas: []metav1.TypeMeta{podMeta},
				Resolve: func(preObject resourcetopo.Object) []resourcetopo.ResourceRelation {
					deployObj, ok := preObject.(*appsv1.Deployment)
					if !ok {
						return nil
					}
					return []resourcetopo.ResourceRelation{{
						PostMeta:      podMeta,
						LabelSelector: deployObj.Spec.Selector,
					}}
				},
				OwnerRelation: []metav1.TypeMeta{podMeta},
				ReverseNotice: nil,
			},
			{
				PreMeta:   serviceMeta,
				PostMetas: []metav1.TypeMeta{podMeta},
				Resolve: func(preObject resourcetopo.Object) []resourcetopo.ResourceRelation {
					svcObj, ok := preObject.(*corev1.Service)
					if !ok {
						return nil
					}
					return []resourcetopo.ResourceRelation{{
						PostMeta: podMeta,
						LabelSelector: &metav1.LabelSelector{
							MatchLabels: svcObj.Spec.Selector,
						},
					}}
				},
				OwnerRelation: nil,
				// Configure to notice pod if upstream service has changed
				ReverseNotice: []metav1.TypeMeta{podMeta},
			},
			{
				PreMeta:   podMeta,
				PostMetas: []metav1.TypeMeta{persistentVolumeClaimMeta},
				Resolve: func(preObject resourcetopo.Object) []resourcetopo.ResourceRelation {
					podObj, ok := preObject.(*corev1.Pod)
					if !ok {
						return nil
					}
					var pvcNames []types.NamespacedName
					for _, v := range podObj.Spec.Volumes {
						if v.PersistentVolumeClaim != nil {
							pvcNames = append(pvcNames, types.NamespacedName{
								Namespace: podObj.Namespace,
								Name:      v.PersistentVolumeClaim.ClaimName,
							})
						}
					}
					return []resourcetopo.ResourceRelation{{
						PostMeta:   persistentVolumeClaimMeta,
						DirectRefs: pvcNames,
					}}
				},
				OwnerRelation: nil,
				ReverseNotice: nil,
			},
		},
	}
}

func buildVirtualSymptomTopology() *resourcetopo.TopologyConfig {
	return &resourcetopo.TopologyConfig{
		GetInformer: getInformer,
		Discoverers: []resourcetopo.VirtualResourceDiscoverer{
			{
				// assume we want to know the relations among pods and symptoms
				// and symptoms will be added to pod's annotation separated by ','.
				PreMeta:  virtualSymptomMeta,
				PostMeta: podMeta,
				Discover: func(preObject resourcetopo.Object) []types.NamespacedName {
					podObj, ok := preObject.(*corev1.Pod)
					if !ok {
						return nil
					}
					podAnno := podObj.ObjectMeta.Annotations
					if len(podAnno) == 0 {
						return nil
					}
					symptoms := strings.Split(podAnno["symptom.diagnose.kusionstack.io"], ",")
					symptomNames := make([]types.NamespacedName, 0, len(symptoms))
					for _, symptom := range symptoms {
						if len(symptom) == 0 {
							continue
						}
						symptomNames = append(symptomNames, types.NamespacedName{
							Namespace: podObj.Namespace,
							Name:      symptom,
						})
					}

					return symptomNames
				},
			},
		},
	}
}

func getInformer(meta metav1.TypeMeta) resourcetopo.Informer {
	var informer cache.Informer
	var err error
	switch meta {
	case deploymentMeta:
		informer, err = mgrCache.GetInformer(backGroundCtx, &appsv1.Deployment{})
	case podMeta:
		informer, err = mgrCache.GetInformer(backGroundCtx, &corev1.Pod{})
	case serviceMeta:
		informer, err = mgrCache.GetInformer(backGroundCtx, &corev1.Service{})
	case persistentVolumeClaimMeta:
		informer, err = mgrCache.GetInformer(backGroundCtx, &corev1.PersistentVolumeClaim{})
	default:
		klog.Errorf("unexpected type meta %v", meta)
		return nil
	}
	if err != nil {
		klog.Errorf("failed to get informer for meta %v: %s", meta, err.Error())
		return nil
	}
	return informer
}

var _ resourcetopo.NodeHandler = &podEventhandler{}

type podEventhandler struct{}

func (p *podEventhandler) OnAdd(info resourcetopo.NodeInfo) {
	klog.Infof("received add event for pod %s", info.NodeInfo().String())
}

func (p *podEventhandler) OnUpdate(info resourcetopo.NodeInfo) {
	klog.Infof("received update event for pod %s", info.NodeInfo().String())
}

func (p *podEventhandler) OnDelete(info resourcetopo.NodeInfo) {
	klog.Infof("received delete event for pod %s", info.NodeInfo().String())
}

func (p *podEventhandler) OnRelatedUpdate(info resourcetopo.NodeInfo) {
	klog.Infof("related node has changed and effected pod %s", info.NodeInfo().String())
	klog.Infof("related pre nodes are %s", nodes2Str(info.GetPreOrders()))
	klog.Infof("related post nodes are %s", nodes2Str(info.GetPostOrders()))
}

var _ resourcetopo.RelationHandler = &symptomPodRelationEventHandler{}

type symptomPodRelationEventHandler struct{}

func (s *symptomPodRelationEventHandler) OnAdd(preOrder resourcetopo.NodeInfo, postOrder resourcetopo.NodeInfo) {
	klog.Infof("received relation add event for %s -> %s", preOrder.NodeInfo().String(), postOrder.NodeInfo().String())
}

func (s *symptomPodRelationEventHandler) OnDelete(preOrder resourcetopo.NodeInfo, postOrder resourcetopo.NodeInfo) {
	klog.Infof("received relation delete event for %s -> %s", preOrder.NodeInfo().String(), postOrder.NodeInfo().String())
}

func nodes2Str(nodes []resourcetopo.NodeInfo) string {
	if len(nodes) == 0 {
		return "nil"
	}
	strBld := strings.Builder{}
	for _, node := range nodes {
		if strBld.Len() > 0 {
			strBld.WriteString(",")
		}
		strBld.WriteString(fmt.Sprintf("{%v:%v}", node.TypeInfo(), node.NodeInfo()))
	}
	return strBld.String()
}
