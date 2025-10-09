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
