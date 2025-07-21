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

func (l LeaderMetricsRunnable) Start(ctx context.Context) error {
	m := metrics.LeaderRunningMetrics{Lease: l.leaseName}
	m.Lead()
	l.logger.Info("enable leader election, start LeaderMetricsRunnable, record leader metrics")
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

func (nl NoneLeaderMetricRunnable) Start(ctx context.Context) error {
	m := metrics.LeaderRunningMetrics{Lease: nl.leaseName}
	if nl.enableLeaderElection {
		m.UnLead()
		nl.logger.Info("enable leader election, start NoneLeaderMetricRunnable, record none leader metrics")
	} else {
		m.Lead()
		nl.logger.Info("disable leader election, start NoneLeaderMetricRunnable, record leader metrics")
	}
	return nil
}
