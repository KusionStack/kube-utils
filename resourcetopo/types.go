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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"
)

// TopologyConfig offers a way to describe the relation among kubernetes or kubernetes-likely resource.
type TopologyConfig struct {
	// GetInformer return a working SharedInformer for objects of meta type
	GetInformer func(meta metav1.TypeMeta) cache.SharedInformer
	Resolvers   []RelationResolver
	Discoverers []VirtualResourceDiscoverer
}

type ManagerConfig struct {
	NodeEventQueueSize     int // size of NodeEvent cache channel, defaults to defaultNodeEventQueueSize
	RelationEventQueueSize int // size of RelationEvent cache channel, defaults to defaultRelationEventQueueSize

	TopologyConfig *TopologyConfig
}

type Manager interface {
	// AddTopologyConfig add new resource topology config to this manager
	AddTopologyConfig(config TopologyConfig) error

	// AddRelationHandler add a new relation handler for the relation change between
	// the two type nodes configured by preMeta and postMeta
	AddRelationHandler(preMeta, postMeta metav1.TypeMeta, handler RelationHandler) error

	// AddNodeHandler add a new node handler for nodes of type configured by meta.
	AddNodeHandler(meta metav1.TypeMeta, handler NodeHandler) error

	// Start to handler relation events and node events.
	// Start can only be called once.
	Start(stopCh <-chan struct{})

	// GetTopoNodeStorage return the ref to TopoNodeStorage that match resource meta.
	GetTopoNodeStorage(meta metav1.TypeMeta) (TopoNodeStorage, error)

	// GetNode return the ref to Node that match the resouorce meta and node's name if existed.
	GetNode(meta metav1.TypeMeta, name types.NamespacedName) (NodeInfo, error)
}

type TopoNodeStorage interface {
	// GetNode return the ref to Node that match the node's name.
	GetNode(name types.NamespacedName) (NodeInfo, error)
}

type NodeInfo interface {
	// TypeInfo return the node's resource type info
	TypeInfo() metav1.TypeMeta

	// NodeInfo return the node's namespaced name
	NodeInfo() types.NamespacedName

	// GetPreOrders return the pre-order node slice for this node
	GetPreOrders() []NodeInfo

	// GetPostOrders return the post-order node slice for this node
	GetPostOrders() []NodeInfo

	// GetPreOrdersWithMeta return the pre-order nodes slice that match this resource meta
	GetPreOrdersWithMeta(meta metav1.TypeMeta) []NodeInfo

	// GetPostOrdersWithMeta return the post-order node slice that match this resource meta
	GetPostOrdersWithMeta(meta metav1.TypeMeta) []NodeInfo
}

// VirtualResourceDiscoverer support to discover a 'virtual' resource from another type and describe the relation between them,
// the virtual resource could be an aggregated resource that not exist in etcd, or a business resource type like order id.
type VirtualResourceDiscoverer struct {
	// PreMeta define the virtual resource type of pre-order in the topology graph,
	// which is not existed in the api-server.
	PreMeta metav1.TypeMeta

	// PostMeta define from which resource type to discover,
	// it may be k8s resource, custom resource, or another virtual resource.
	PostMeta metav1.TypeMeta

	// Discover need to resolve the inputted postObject and return pre-order object names of PostMeta .
	Discover func(postObject Object) []types.NamespacedName
}

// RelationResolver offers a way to describe relation among objects of PreMeta resource type and PostMetas resource types.
type RelationResolver struct {
	// PreMeta define the resource type of pre-order in the topology graph.
	PreMeta metav1.TypeMeta

	// PostMetas define a series of resource types of post-orders in the topology graph.
	PostMetas []metav1.TypeMeta

	// Resolve need to resolve the inputted preObject and return ResourceRelations of PostMeta types.
	Resolve func(preObject Object) []ResourceRelation

	// OwnerRelation offered a way to configure kubernetes owner-reference relation,
	// for workload-pod relation, selector is not enough to show the relation between them.
	// If post meta configured in it, will only add a relation if post node has pre's owner configured.
	// could be nil if there is no need.
	OwnerRelation []metav1.TypeMeta

	// ReverseNotice offered a way to configure notice propagate direction,
	// for pod diagnose scenario, pod's status want to include upstream service objects.
	// If post meta configured in it, will notice post node if pre node has changed,
	// and relevantly, no longer to notice pre node when post node change.
	// could be nil if there is no need.
	ReverseNotice []metav1.TypeMeta
}

// ResourceRelation is generated by Resolve function set in RelationResolver,
// to express the relation between a pre-object and its post objects of PostMeta type.
type ResourceRelation struct {
	// PostMeta restricted the post object(s) resource type.
	PostMeta metav1.TypeMeta

	// DirectRefs offers a direct reference way for pod-pvc type relation, could be nil if it's empty.
	DirectRefs []types.NamespacedName

	// LabelSelector offers a kubernetes label-selector way for svc-pod type relation, could be nil if it's empty.
	LabelSelector *metav1.LabelSelector
}

type RelationHandler interface {
	// OnAdd is called after relation newly added between preOrder and postOrder
	OnAdd(preOrder NodeInfo, postOrder NodeInfo)

	// OnDelete is called after relation deleted between preOrder and postOrder
	OnDelete(preOrder NodeInfo, postOrder NodeInfo)
}

type NodeHandler interface {
	// OnAdd is called when the node related resource object is added
	OnAdd(info NodeInfo)

	// OnUpdate is called when the node related resource object is updated
	OnUpdate(info NodeInfo)

	// OnDelete is called when the node related resource object is deleted
	OnDelete(info NodeInfo)

	// OnRelatedUpdate is called when the nodes'
	// post-order(or pre-order if ReverseNotice configured) object is updated
	OnRelatedUpdate(info NodeInfo)
}

// Object define get namespace/name/labels for now,
// func name need to keep with metav1.Object.
type Object interface {
	GetName() string
	GetNamespace() string
	GetLabels() map[string]string
	GetOwnerReferences() []metav1.OwnerReference
}
