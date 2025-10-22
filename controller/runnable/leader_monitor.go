/*
Copyright 2025 The KusionStack Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package runnable

import (
	"context"

	"github.com/go-logr/logr"

	"kusionstack.io/kube-utils/metrics"
)

// LeaderMetricsRunnable is a runnable that records the leader metrics
type LeaderMetricsRunnable struct {
	logger    logr.Logger
	leaseName string
}

func (l LeaderMetricsRunnable) NeedLeaderElection() bool {
	return true
}

func (l LeaderMetricsRunnable) Start(_ context.Context) error {
	m := metrics.LeaderRunningMetrics{Lease: l.leaseName}
	m.Lead()
	l.logger.Info("leader election enabled, start LeaderMetricsRunnable, record leader metrics")
	return nil
}

// NoneLeaderMetricRunnable is a runnable that records the none leader metrics
type NoneLeaderMetricRunnable struct {
	logger               logr.Logger
	leaseName            string
	enableLeaderElection bool
}

func (nl NoneLeaderMetricRunnable) NeedLeaderElection() bool {
	return false
}

func (nl NoneLeaderMetricRunnable) Start(_ context.Context) error {
	m := metrics.LeaderRunningMetrics{Lease: nl.leaseName}
	if nl.enableLeaderElection {
		m.UnLead()
		nl.logger.Info("leader election enabled, start NoneLeaderMetricRunnable, record none leader metrics")
	} else {
		m.Lead()
		nl.logger.Info("leader election disabled, start NoneLeaderMetricRunnable, record leader metrics")
	}
	return nil
}
