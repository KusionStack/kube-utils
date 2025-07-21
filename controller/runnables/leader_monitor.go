package runnables

import (
	"context"

	"kusionstack.io/kube-utils/metrics"
)

// LeaderMetricsRunnable is a runnable that records the leader metrics
type LeaderMetricsRunnable struct {
	leaseName string
}

func (l LeaderMetricsRunnable) NeedLeaderElection() bool {
	return true
}

func (l LeaderMetricsRunnable) Start(_ context.Context) error {
	m := metrics.LeaderRunningMetrics{Lease: l.leaseName}
	m.Lead()
	return nil
}

// NoneLeaderMetricRunnable is a runnable that records the none leader metrics
type NoneLeaderMetricRunnable struct {
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
	} else {
		m.Lead()
	}
	return nil
}
