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

package metrics

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

const (
	MultiClusterSubSystem = "multicluster"
	CacheCount            = "cache_count"
	ClientCount           = "client_count"
	ClusterEventCount     = "cluster_event_count"
	InvalidClusterCount   = "invalid_cluster_count"
)

var (
	cacheCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Subsystem: MultiClusterSubSystem,
		Name:      CacheCount,
		Help:      "Number of Cache methods invoked",
	}, []string{"cluster", "method", "code"})

	clientCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Subsystem: MultiClusterSubSystem,
		Name:      ClientCount,
		Help:      "Number of Client methods invoked",
	}, []string{"cluster", "method", "code"})

	clusterEventCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Subsystem: MultiClusterSubSystem,
		Name:      ClusterEventCount,
		Help:      "Number of cluster events",
	}, []string{"cluster", "event", "success"})

	invalidClusterCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Subsystem: MultiClusterSubSystem,
		Name:      InvalidClusterCount,
		Help:      "Number of invalid clusters for Client and Cache",
	}, []string{"method", "cluster"})
)

func init() {
	metrics.Registry.MustRegister(cacheCounter)
	metrics.Registry.MustRegister(clientCounter)
	metrics.Registry.MustRegister(clusterEventCounter)
	metrics.Registry.MustRegister(invalidClusterCounter)
}

func NewCacheCountMetrics(cluster, method string, err error) prometheus.Counter {
	return cacheCounter.WithLabelValues(cluster, method, CodeForError(err))
}

func NewClientCountMetrics(cluster, method string, err error) prometheus.Counter {
	return clientCounter.WithLabelValues(cluster, method, CodeForError(err))
}

func NewClusterEventCountMetrics(cluster, event, success string) prometheus.Counter {
	return clusterEventCounter.WithLabelValues(cluster, event, success)
}

func NewInvalidClusterCounterMetrics(method, cluster string) prometheus.Counter {
	return invalidClusterCounter.WithLabelValues(method, cluster)
}

func CodeForError(err error) string {
	if err == nil {
		return "200"
	}

	switch t := err.(type) {
	case errors.APIStatus:
		return strconv.Itoa(int(t.Status().Code))
	}
	return "0"
}
