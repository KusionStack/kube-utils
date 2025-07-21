package runnable

import (
	"github.com/go-logr/logr"
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
	addRunnable(opts, mgr.GetLogger())
	for _, runnable := range runnable {
		if err := mgr.Add(runnable); err != nil {
			return err
		}
	}
	return nil
}

func addRunnable(opts controllerruntime.Options, logger logr.Logger) {
	runnable = append(runnable,
		&LeaderMetricsRunnable{leaseName: opts.LeaderElectionID, logger: logger},
		&NoneLeaderMetricRunnable{enableLeaderElection: opts.LeaderElection, leaseName: opts.LeaderElectionID, logger: logger},
	)
}
