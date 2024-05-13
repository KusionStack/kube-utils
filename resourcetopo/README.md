# Resourcetopo

Resourcetopo is an open source util to manager the resource topology in your kubernetes cluster. 
It can help you to obtain the resource topology relation and trigger related events when the topology graph has changed.

## Quick Start

The running code of this part is placed in examples/ dir.

### Create a Resourcetopo Manager

We need to create a resourcetopo manager instance before all.

```go
topoMgr, err := resourcetopo.NewResourcesTopoManager(
	resourcetopo.ManagerConfig{
		NodeEventQueueSize:     1024,
		RelationEventQueueSize: 1024,
		TopologyConfig: nil,
	},
)
```

### Setup Topology Config

We need to set up the topology config for the resourcetopo manager.
The topology config is used to describe the resource topology in your kubernetes cluster.

```go

func buildExampleTopologyConfig() *resourcetopo.TopologyConfig {
	return &resourcetopo.TopologyConfig{
		GetInformer: getInformer, // getInformer function resolve meta and return the related informer.
		Resolvers: []resourcetopo.RelationResolver{
			{
				PreMeta: deployMeta,
				PostMetas: []metav1.TypeMeta{
					podMeta,
				},
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
				OwnerRelation: []metav1.TypeMeta{
					podMeta,
				},
				ReverseNotice: nil,
			},
		},
		
}			
```
### Add Event Handler

This is an optional step, we can add event handlers to handle the node or relation change events.

```go
func main() {
	// ...
	err = topoManager.AddNodeHandler(podMeta, &podEventhandler{})
	err = topoManager.AddRelationHandler(virtualSymptomMeta, podMeta, &symptomPodRelationEventHandler{})
}

// podEventHandler is an implementation of the interface NodeHandler, which is designed to handle add/update/delete events of resources.
// Specially, we define the OnRelatedUpdate function, to handle event when the related node has been modified.
// For example, if a new pod has been added, except for a pod add event, a deployment related update event will
// also be generated and call the registered handler.
var _ resourcetopo.NodeHandler = &podEventhandler{}

// symptomPodRelationEventHandler is an impl of preDefined RelationHandler, included function to handler relation add/delete Event.
var _ resourcetopo.RelationHandler = &symptomPodRelationEventHandler{}

```

### Start Manager

After configuration settled, we can call Start func and wait for the node/relation events coming.

```go
func main() {
	// ...
	topoManager.Start()
}
```

### Receive and Handle Events

In the base example demo, we just print out the meta info, but you can do more things according to specific scenario.
Part of the demo output content is as follows:

```
I0509 10:50:09.967769   45952 base_example.go:235] received add event for pod etcd/etcd-0
......
I0509 10:50:10.835929   45952 base_example.go:247] related node has changed and effected pod etcd/etcd-0
I0509 10:50:10.835938   45952 base_example.go:248] related pre nodes are {{Service core/v1}:etcd/etcd}
I0509 10:50:10.835941   45952 base_example.go:249] related post nodes are {{PersistentVolumeClaim core/v1}:etcd/etcd-data-dir-etcd-0}
```
