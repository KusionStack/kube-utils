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
