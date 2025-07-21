package runnables

import (
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"kusionstack.io/kube-utils/metrics"
)

var runnable []manager.Runnable

// InitializeRunnable initialize runnable for manager
func InitializeRunnable(mgr manager.Manager, opts controllerruntime.Options) error {
	// register leader metrics
	metrics.RegisterLeaderRunningMetrics()
	// register runnable
	AddRunnable(opts)
	for _, runnable := range runnable {
		if err := mgr.Add(runnable); err != nil {
			return err
		}
	}
	return nil
}

func AddRunnable(opts controllerruntime.Options) {
	runnable = append(runnable,
		&LeaderMetricsRunnable{leaseName: opts.LeaderElectionID},
		&NoneLeaderMetricRunnable{enableLeaderElection: opts.LeaderElection, leaseName: opts.LeaderElectionID},
	)
}
