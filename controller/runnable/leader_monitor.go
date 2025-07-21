package runnable

import (
	"context"

	"github.com/go-logr/logr"

	"kusionstack.io/kube-utils/metrics"
)

// LeaderMetricsRunnable is a runnable that records the leader metrics
type LeaderMetricsRunnable struct {
	leaseName string
}

func (l LeaderMetricsRunnable) NeedLeaderElection() bool {
	return true
}

func (l LeaderMetricsRunnable) Start(ctx context.Context) error {
	logger := logr.FromContextOrDiscard(ctx)
	m := metrics.LeaderRunningMetrics{Lease: l.leaseName}
	m.Lead()
	logger.Info("enable leader election, record leader metrics")
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

func (nl NoneLeaderMetricRunnable) Start(ctx context.Context) error {
	logger := logr.FromContextOrDiscard(ctx)
	m := metrics.LeaderRunningMetrics{Lease: nl.leaseName}
	if nl.enableLeaderElection {
		m.UnLead()
		logger.Info("enable leader election, record none leader metrics")
	} else {
		m.Lead()
		logger.Info("disable leader election, record leader metrics")
	}
	return nil
}
