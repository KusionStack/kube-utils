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
	"container/list"
	"fmt"
	"strings"

	"golang.org/x/exp/slices"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"kusionstack.io/kube-utils/multicluster/clusterinfo"
)

func generateMetaKey(meta metav1.TypeMeta) string {
	return fmt.Sprintf("%s/%s", meta.APIVersion, meta.Kind)
}

func generateKey(APIVersion, Kind string) string {
	return fmt.Sprintf("%s/%s", APIVersion, Kind)
}

func checkNode(s *nodeStorage, stack *list.List, visited map[string]bool) error {
	stack.PushBack(s.metaKey)

	for _, next := range s.preOrderResources {
		if existInList(stack, next.metaKey) {
			return fmt.Errorf("DAG check for resource %s failed", next.metaKey)
		} else {
			visited[next.metaKey] = true
			if err := checkNode(next, stack, visited); err != nil {
				return err
			}
		}
	}

	stack.Remove(stack.Back())
	return nil
}

func rangeNodeList(l *list.List, nodeFunc func(n *nodeInfo)) {
	if l == nil || l.Len() == 0 {
		return
	}
	for ele := l.Front(); ele != nil; ele = ele.Next() {
		nodeFunc(ele.Value.(*nodeInfo))
	}
}

func transformToNodeSliceWithFilters(l *list.List, filters ...func(*nodeInfo) bool) []NodeInfo {
	if l == nil || l.Len() == 0 {
		return []NodeInfo{}
	}
	var res []NodeInfo
LOOP:
	for ele := l.Front(); ele != nil; ele = ele.Next() {
		n := ele.Value.(*nodeInfo)
		if len(filters) > 0 {
			for _, filter := range filters {
				if !filter(n) {
					continue LOOP
				}
			}
		}
		res = append(res, n)
	}
	return res
}

func removeFromList(l *list.List, v interface{}) bool {
	if l == nil || l.Len() == 0 {
		return false
	}
	for ele := l.Front(); ele != nil; ele = ele.Next() {
		if ele.Value == v {
			l.Remove(ele)
			return true
		}
	}
	return false
}

func existInList(l *list.List, value interface{}) bool {
	if l == nil || l.Len() == 0 {
		return false
	}
	for ele := l.Front(); ele != nil; ele = ele.Next() {
		if ele.Value == value {
			return true
		}
	}
	return false
}

func compareResourceRelation(a, b ResourceRelation) int {
	var res int
	if res = strings.Compare(a.PostMeta.APIVersion, b.PostMeta.APIVersion); res != 0 {
		return res
	}
	if res = strings.Compare(a.PostMeta.Kind, b.PostMeta.Kind); res != 0 {
		return res
	}

	if res = len(a.DirectRefs) - len(b.DirectRefs); res != 0 {
		return res
	}
	slices.SortFunc(a.DirectRefs, compareNodeName)
	slices.SortFunc(b.DirectRefs, compareNodeName)
	for i := 0; i < len(a.DirectRefs); i++ {
		if res = strings.Compare(a.DirectRefs[i].Namespace, b.DirectRefs[i].Namespace); res != 0 {
			return res
		}
		if res = strings.Compare(a.DirectRefs[i].Name, b.DirectRefs[i].Name); res != 0 {
			return res
		}
	}

	if res = strings.Compare(a.Cluster, b.Cluster); res != 0 {
		return res
	}

	if a.LabelSelector == nil && b.LabelSelector == nil {
		return 0
	} else if a.LabelSelector == nil && b.LabelSelector != nil {
		return -1
	} else if a.LabelSelector != nil && b.LabelSelector == nil {
		return 1
	} else if res = a.LabelSelector.Size() - b.LabelSelector.Size(); res != 0 {
		return res
	} else {
		return strings.Compare(a.LabelSelector.String(), b.LabelSelector.String())
	}

}

func compareNodeName(a, b types.NamespacedName) int {
	if res := strings.Compare(a.Namespace, b.Namespace); res != 0 {
		return res
	} else {
		return strings.Compare(a.Name, b.Name)
	}
}

// sortedSlicesCompare implement an ordered array compare algorithm.
func sortedSlicesCompare[S ~[]E, E any](slice1, slice2 S, diffFunc1, diffFunc2 func(E), compareFunc func(E, E) int) {
	i, j := 0, 0
	for i < len(slice1) && j < len(slice2) {
		res := compareFunc(slice1[i], slice2[j])
		if res == 0 {
			i++
			j++
			continue
		} else if res > 0 {
			diffFunc2(slice2[j])
			j++
		} else {
			diffFunc1(slice1[i])
			i++
		}
	}
	for i < len(slice1) {
		diffFunc1(slice1[i])
		i++
	}
	for j < len(slice2) {
		diffFunc2(slice2[j])
		j++
	}
}

func typeEqual(t1, t2 metav1.TypeMeta) bool {
	return t1.Kind == t2.Kind && t1.APIVersion == t2.APIVersion
}

func getObjectCluster(obj Object) string {
	if labels := obj.GetLabels(); labels != nil {
		if cluster, ok := labels[clusterinfo.ClusterLabelKey]; ok {
			return cluster
		}
	}
	return ""
}
