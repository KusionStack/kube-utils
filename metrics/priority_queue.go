/**
 * Copyright 2023 KusionStack Authors.
 * Copyright 2019 The Kubernetes Authors.
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
	k8smetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	"kusionstack.io/kube-utils/controller/workqueue"
)

// Package prometheus sets the priority queue PriorityQueueMetricsProvider to produce
// prometheus metrics. To use this package, you just have to import it.

// Metrics subsystem and keys used by the priority queue.
const (
	PriorityQueueSubsystem     = "priority_queue"
	DepthKey                   = "depth"
	AddsKey                    = "adds_total"
	QueueLatencyKey            = "queue_duration_seconds"
	WorkDurationKey            = "work_duration_seconds"
	UnfinishedWorkKey          = "unfinished_work_seconds"
	LongestRunningProcessorKey = "longest_running_processor_seconds"
)

var (
	depth = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: PriorityQueueSubsystem,
		Name:      DepthKey,
		Help:      "Current depth of priority queue",
	}, []string{"name", "priority"})

	adds = prometheus.NewCounterVec(prometheus.CounterOpts{
		Subsystem: PriorityQueueSubsystem,
		Name:      AddsKey,
		Help:      "Total number of adds handled by priority queue",
	}, []string{"name", "priority"})

	latency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Subsystem: PriorityQueueSubsystem,
		Name:      QueueLatencyKey,
		Help:      "How long in seconds an item stays in priority queue before being requested",
		Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.15, 0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0,
			1.25, 1.5, 1.75, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5, 6, 7, 8, 9, 10, 15, 20, 25, 30, 40, 50, 60},
	}, []string{"name", "priority"})

	workDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Subsystem: PriorityQueueSubsystem,
		Name:      WorkDurationKey,
		Help:      "How long in seconds processing an item from priority queue takes.",
		Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.15, 0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0,
			1.25, 1.5, 1.75, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5, 6, 7, 8, 9, 10, 15, 20, 25, 30, 40, 50, 60},
	}, []string{"name", "priority"})

	unfinished = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: PriorityQueueSubsystem,
		Name:      UnfinishedWorkKey,
		Help: "How many seconds of work has been done that " +
			"is in progress and hasn't been observed by work_duration. Large " +
			"values indicate stuck threads. One can deduce the number of stuck " +
			"threads by observing the rate at which this increases.",
	}, []string{"name"})

	longestRunningProcessor = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: PriorityQueueSubsystem,
		Name:      LongestRunningProcessorKey,
		Help: "How many seconds has the longest running " +
			"processor for priority queue been running.",
	}, []string{"name"})
)

type prometheusMetricsProvider struct {
}

// RegisterPriorityQueueMetrics registers the prometheus metrics for the priority queue.
// Must be called before any priority queue is created.
func RegisterPriorityQueueMetrics() {
	k8smetrics.Registry.MustRegister(depth)
	k8smetrics.Registry.MustRegister(adds)
	k8smetrics.Registry.MustRegister(latency)
	k8smetrics.Registry.MustRegister(workDuration)
	k8smetrics.Registry.MustRegister(unfinished)
	k8smetrics.Registry.MustRegister(longestRunningProcessor)

	workqueue.SetPriorityQueueMetricsProvider(prometheusMetricsProvider{})
}

type DepthMetric struct {
	name string
}

func (g *DepthMetric) Inc(i int) {
	depth.WithLabelValues(g.name, strconv.Itoa(i)).Inc()
}

func (g *DepthMetric) Dec(i int) {
	depth.WithLabelValues(g.name, strconv.Itoa(i)).Dec()
}

func (prometheusMetricsProvider) NewDepthMetric(name string) workqueue.GaugeWithPriorityMetric {
	return &DepthMetric{name: name}
}

type AddsMetric struct {
	name string
}

func (c *AddsMetric) Inc(i int) {
	adds.WithLabelValues(c.name, strconv.Itoa(i)).Inc()
}

func (prometheusMetricsProvider) NewAddsMetric(name string) workqueue.CounterWithPriorityMetric {
	return &AddsMetric{name: name}
}

type LatencyMetric struct {
	name string
}

func (h *LatencyMetric) Observe(i int, v float64) {
	latency.WithLabelValues(h.name, strconv.Itoa(i)).Observe(v)
}

func (prometheusMetricsProvider) NewLatencyMetric(name string) workqueue.HistogramWithPriorityMetric {
	return &LatencyMetric{name: name}
}

type WorkDurationMetric struct {
	name string
}

func (h *WorkDurationMetric) Observe(i int, v float64) {
	workDuration.WithLabelValues(h.name, strconv.Itoa(i)).Observe(v)
}

func (prometheusMetricsProvider) NewWorkDurationMetric(name string) workqueue.HistogramWithPriorityMetric {
	return &WorkDurationMetric{name: name}
}

func (prometheusMetricsProvider) NewUnfinishedWorkSecondsMetric(name string) workqueue.SettableGaugeWithPriorityMetric {
	return unfinished.WithLabelValues(name)
}

func (prometheusMetricsProvider) NewLongestRunningProcessorSecondsMetric(name string) workqueue.SettableGaugeWithPriorityMetric {
	return longestRunningProcessor.WithLabelValues(name)
}
