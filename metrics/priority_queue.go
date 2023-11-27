/**
 * Copyright 2023 KusionStack Authors.
 * Copyright 2019 The Kubernetes Authors.
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
	k8smetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	"kusionstack.io/kube-utils/controller/workqueue"
)

// Package prometheus sets the priority queue PriorityQueueMetricsProvider to produce
// prometheus metrics. To use this package, you just have to import it.

// Metrics subsystem and keys used by the priority queue.
const (
	WorkQueueSubsystem         = "priority_queue"
	DepthKey                   = "depth"
	AddsKey                    = "adds_total"
	QueueLatencyKey            = "queue_duration_seconds"
	WorkDurationKey            = "work_duration_seconds"
	UnfinishedWorkKey          = "unfinished_work_seconds"
	LongestRunningProcessorKey = "longest_running_processor_seconds"
	RetriesKey                 = "retries_total"
)

var (
	depth = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: WorkQueueSubsystem,
		Name:      DepthKey,
		Help:      "Current depth of workqueue",
	}, []string{"name", "priority"})

	adds = prometheus.NewCounterVec(prometheus.CounterOpts{
		Subsystem: WorkQueueSubsystem,
		Name:      AddsKey,
		Help:      "Total number of adds handled by workqueue",
	}, []string{"name", "priority"})

	latency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Subsystem: WorkQueueSubsystem,
		Name:      QueueLatencyKey,
		Help:      "How long in seconds an item stays in workqueue before being requested",
		Buckets:   prometheus.ExponentialBuckets(10e-9, 10, 10),
	}, []string{"name", "priority"})

	workDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Subsystem: WorkQueueSubsystem,
		Name:      WorkDurationKey,
		Help:      "How long in seconds processing an item from workqueue takes.",
		Buckets:   prometheus.ExponentialBuckets(10e-9, 10, 10),
	}, []string{"name", "priority"})

	unfinished = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: WorkQueueSubsystem,
		Name:      UnfinishedWorkKey,
		Help: "How many seconds of work has been done that " +
			"is in progress and hasn't been observed by work_duration. Large " +
			"values indicate stuck threads. One can deduce the number of stuck " +
			"threads by observing the rate at which this increases.",
	}, []string{"name", "priority"})

	longestRunningProcessor = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: WorkQueueSubsystem,
		Name:      LongestRunningProcessorKey,
		Help: "How many seconds has the longest running " +
			"processor for workqueue been running.",
	}, []string{"name", "priority"})
)

type prometheusMetricsProvider struct {
}

func init() {
	k8smetrics.Registry.MustRegister(depth)
	k8smetrics.Registry.MustRegister(adds)
	k8smetrics.Registry.MustRegister(latency)
	k8smetrics.Registry.MustRegister(workDuration)
	k8smetrics.Registry.MustRegister(unfinished)
	k8smetrics.Registry.MustRegister(longestRunningProcessor)

	workqueue.SetPriorityQueueMetricsProvider(prometheusMetricsProvider{})
}

type GaugeWithPriorityMetric struct {
	name string
}

func (g *GaugeWithPriorityMetric) Inc(i int) {
	depth.WithLabelValues(g.name, strconv.Itoa(i)).Inc()
}

func (g *GaugeWithPriorityMetric) Dec(i int) {
	depth.WithLabelValues(g.name, strconv.Itoa(i)).Dec()
}

func (prometheusMetricsProvider) NewDepthMetric(name string) workqueue.GaugeWithPriorityMetric {
	return &GaugeWithPriorityMetric{name: name}
}

type CounterWithPriorityMetric struct {
	name string
}

func (c *CounterWithPriorityMetric) Inc(i int) {
	adds.WithLabelValues(c.name, strconv.Itoa(i)).Inc()
}

func (prometheusMetricsProvider) NewAddsMetric(name string) workqueue.CounterWithPriorityMetric {
	return &CounterWithPriorityMetric{name: name}
}

type HistogramWithPriorityMetric struct {
	name string
}

func (h *HistogramWithPriorityMetric) Observe(i int, v float64) {
	latency.WithLabelValues(h.name, strconv.Itoa(i)).Observe(v)
}

func (prometheusMetricsProvider) NewLatencyMetric(name string) workqueue.HistogramWithPriorityMetric {
	return &HistogramWithPriorityMetric{name: name}
}

func (prometheusMetricsProvider) NewWorkDurationMetric(name string) workqueue.HistogramWithPriorityMetric {
	return &HistogramWithPriorityMetric{name: name}
}

func (prometheusMetricsProvider) NewUnfinishedWorkSecondsMetric(name string) workqueue.SettableGaugeWithPriorityMetric {
	return unfinished.WithLabelValues(name)
}

func (prometheusMetricsProvider) NewLongestRunningProcessorSecondsMetric(name string) workqueue.SettableGaugeWithPriorityMetric {
	return longestRunningProcessor.WithLabelValues(name)
}
