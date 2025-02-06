# Extractor

Extract specific field from JSON-like data and **output not only the field value but also its upstream structure**.

A typical use case is to trim k8s objects in `TransformingInformer` to save informer memory.

Please refer to [JSONPath Support](https://kubernetes.io/docs/reference/kubectl/jsonpath/) to see JSONPath usage.

## Example

Code:

```go
package main

import (
	"encoding/json"
	"fmt"

	"kusionstack.io/kube-utils/extractor"
)

var pod = []byte(`{
    "apiVersion": "v1",
    "kind": "Pod",
    "metadata": {
        "labels": {
            "name": "pause",
            "app": "pause"
        },
        "name": "pause",
        "namespace": "default"
    },
    "spec": {
        "containers": [
            {
                "image": "registry.k8s.io/pause:3.8",
                "imagePullPolicy": "IfNotPresent",
                "name": "pause1"
            },
            {
                "image": "registry.k8s.io/pause:3.8",
                "imagePullPolicy": "IfNotPresent",
                "name": "pause2"
            }
        ]
    }
}`)

func printJSON(data interface{}) {
	bytes, _ := json.Marshal(data)
	fmt.Println(string(bytes))
}

func main() {
	var podData map[string]interface{}
	json.Unmarshal(pod, &podData)

	kindPath := "{.kind}"
	kindExtractor, _ := extractor.New([]string{kindPath})

	kind, _ := kindExtractor.Extract(podData)
	printJSON(kind)

	nameImagePath := "{.spec.containers[*]['name', 'image']}"
	nameImageExtractor, _ := extractor.New([]string{nameImagePath})

	nameImage, _ := nameImageExtractor.Extract(podData)
	printJSON(nameImage)

	mergeExtractor, _ := extractor.New([]string{kindPath, nameImagePath})
	merged, _ := mergeExtractor.Extract(podData)
	printJSON(merged)
}
```

Output:

```plain
{"kind":"Pod"}
{"spec":{"containers":[{"image":"registry.k8s.io/pause:3.8","name":"pause1"},{"image":"registry.k8s.io/pause:3.8","name":"pause2"}]}}
{"kind":"Pod","spec":{"containers":[{"image":"registry.k8s.io/pause:3.8","name":"pause1"},{"image":"registry.k8s.io/pause:3.8","name":"pause2"}]}}
```

## Note

The merge behavior on the list is replacing. Therefore, if you retrieve the container name and image separately and merge them, the resulting output will not contain the image.

Code:

```go
    ...
	namePath := "{.spec.containers[*].name}"
	imagePath := "{.spec.containers[*].image}"

	mergeExtractor, _ = extractor.New([]string{imagePath, namePath})
	merged, _ = mergeExtractor.Extract(podData)
	printJSON(merged)
    ...
```

Output:

```plain
{"spec":{"containers":[{"name":"pause1"},{"name":"pause2"}]}}
```
