/**
 * Copyright 2023 KusionStack Authors.
 * Copyright 2015 The Kubernetes Authors.
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

package workqueue

import (
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/util/clock"
)

// This file provides abstractions for setting the provider (e.g., prometheus)
// of metrics.

type priotityQueueMetrics interface {
	add(item t, priority int)
	get(item t, priority int)
	done(item t, priority int)
	updateUnfinishedWork()
}

// GaugeWithPriorityMetric represents a single numerical value that can arbitrarily go up
// and down.
type GaugeWithPriorityMetric interface {
	Inc(int)
	Dec(int)
}

// SettableGaugeWithPriorityMetric represents a single numerical value that can arbitrarily go up
// and down. (Separate from GaugeWithPriorityMetric to preserve backwards compatibility.)
type SettableGaugeWithPriorityMetric interface {
	Set(float64)
}

// CounterWithPriorityMetric represents a single numerical value that only ever
// goes up.
type CounterWithPriorityMetric interface {
	Inc(int)
}

// SummaryWithPriorityMetric captures individual observations.
type SummaryWithPriorityMetric interface {
	Observe(float64, int)
}

// HistogramWithPriorityMetric counts individual observations.
type HistogramWithPriorityMetric interface {
	Observe(int, float64)
}

type noopPriorityQueueMetric struct{}

func (noopPriorityQueueMetric) Inc(int)              {}
func (noopPriorityQueueMetric) Dec(int)              {}
func (noopPriorityQueueMetric) Set(float64)          {}
func (noopPriorityQueueMetric) Observe(int, float64) {}

// defaultPriorityQueueMetrics expects the caller to lock before setting any metrics.
type defaultPriorityQueueMetrics struct {
	clock clock.Clock

	// current depth of a workqueue
	depth GaugeWithPriorityMetric
	// total number of adds handled by a workqueue
	adds CounterWithPriorityMetric
	// how long an item stays in a workqueue
	latency HistogramWithPriorityMetric
	// how long processing an item from a workqueue takes
	workDuration         HistogramWithPriorityMetric
	addTimes             map[t]time.Time
	processingStartTimes map[t]time.Time

	// how long have current threads been working?
	unfinishedWorkSeconds   SettableGaugeWithPriorityMetric
	longestRunningProcessor SettableGaugeWithPriorityMetric
}

func (m *defaultPriorityQueueMetrics) add(item t, priority int) {
	if m == nil {
		return
	}

	m.adds.Inc(priority)
	m.depth.Inc(priority)
	if _, exists := m.addTimes[item]; !exists {
		m.addTimes[item] = m.clock.Now()
	}
}

func (m *defaultPriorityQueueMetrics) get(item t, priority int) {
	if m == nil {
		return
	}

	m.depth.Dec(priority)
	m.processingStartTimes[item] = m.clock.Now()
	if startTime, exists := m.addTimes[item]; exists {
		m.latency.Observe(priority, m.sinceInSeconds(startTime))
		delete(m.addTimes, item)
	}
}

func (m *defaultPriorityQueueMetrics) done(item t, priority int) {
	if m == nil {
		return
	}

	if startTime, exists := m.processingStartTimes[item]; exists {
		m.workDuration.Observe(priority, m.sinceInSeconds(startTime))
		delete(m.processingStartTimes, item)
	}
}

func (m *defaultPriorityQueueMetrics) updateUnfinishedWork() {
	// Note that a summary metric would be better for this, but prometheus
	// doesn't seem to have non-hacky ways to reset the summary metrics.
	var total float64
	var oldest float64
	for _, t := range m.processingStartTimes {
		age := m.sinceInSeconds(t)
		total += age
		if age > oldest {
			oldest = age
		}
	}
	m.unfinishedWorkSeconds.Set(total)
	m.longestRunningProcessor.Set(oldest)
}

type noPriorityQueueMetrics struct{}

func (noPriorityQueueMetrics) add(item t, priority int)  {}
func (noPriorityQueueMetrics) get(item t, priority int)  {}
func (noPriorityQueueMetrics) done(item t, priority int) {}
func (noPriorityQueueMetrics) updateUnfinishedWork()     {}

// Gets the time since the specified start in seconds.
func (m *defaultPriorityQueueMetrics) sinceInSeconds(start time.Time) float64 {
	return m.clock.Since(start).Seconds()
}

// PriorityQueueMetricsProvider generates various metrics used by the priority queue.
type PriorityQueueMetricsProvider interface {
	NewDepthMetric(name string) GaugeWithPriorityMetric
	NewAddsMetric(name string) CounterWithPriorityMetric
	NewLatencyMetric(name string) HistogramWithPriorityMetric
	NewWorkDurationMetric(name string) HistogramWithPriorityMetric
	NewUnfinishedWorkSecondsMetric(name string) SettableGaugeWithPriorityMetric
	NewLongestRunningProcessorSecondsMetric(name string) SettableGaugeWithPriorityMetric
}

type noopPriorityQueueMetricsProvider struct{}

func (mp noopPriorityQueueMetricsProvider) NewDepthMetric(name string) GaugeWithPriorityMetric {
	return noopPriorityQueueMetric{}
}

func (mp noopPriorityQueueMetricsProvider) NewAddsMetric(name string) CounterWithPriorityMetric {
	return noopPriorityQueueMetric{}
}

func (mp noopPriorityQueueMetricsProvider) NewLatencyMetric(name string) HistogramWithPriorityMetric {
	return noopPriorityQueueMetric{}
}

func (mp noopPriorityQueueMetricsProvider) NewWorkDurationMetric(name string) HistogramWithPriorityMetric {
	return noopPriorityQueueMetric{}
}

func (mp noopPriorityQueueMetricsProvider) NewUnfinishedWorkSecondsMetric(name string) SettableGaugeWithPriorityMetric {
	return noopPriorityQueueMetric{}
}

func (mp noopPriorityQueueMetricsProvider) NewLongestRunningProcessorSecondsMetric(name string) SettableGaugeWithPriorityMetric {
	return noopPriorityQueueMetric{}
}

var globalPriorityQueueMetricsFactory = priorityQueueMetricsFactory{
	metricsProvider: noopPriorityQueueMetricsProvider{},
}

type priorityQueueMetricsFactory struct {
	metricsProvider PriorityQueueMetricsProvider

	onlyOnce sync.Once
}

func (f *priorityQueueMetricsFactory) setProvider(mp PriorityQueueMetricsProvider) {
	f.onlyOnce.Do(func() {
		f.metricsProvider = mp
	})
}

func (f *priorityQueueMetricsFactory) newPriorityQueueMetrics(name string, clock clock.Clock) priotityQueueMetrics {
	mp := f.metricsProvider
	if len(name) == 0 || mp == (noopPriorityQueueMetricsProvider{}) {
		return noPriorityQueueMetrics{}
	}
	return &defaultPriorityQueueMetrics{
		clock:                   clock,
		depth:                   mp.NewDepthMetric(name),
		adds:                    mp.NewAddsMetric(name),
		latency:                 mp.NewLatencyMetric(name),
		workDuration:            mp.NewWorkDurationMetric(name),
		unfinishedWorkSeconds:   mp.NewUnfinishedWorkSecondsMetric(name),
		longestRunningProcessor: mp.NewLongestRunningProcessorSecondsMetric(name),
		addTimes:                map[t]time.Time{},
		processingStartTimes:    map[t]time.Time{},
	}
}

// SetPriorityQueueMetricsProvider sets the metrics provider for all subsequently created work
// queues. Only the first call has an effect.
func SetPriorityQueueMetricsProvider(metricsProvider PriorityQueueMetricsProvider) {
	globalPriorityQueueMetricsFactory.setProvider(metricsProvider)
}
