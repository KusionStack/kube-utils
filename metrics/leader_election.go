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

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

var leaderRunningCount = "leader_running_gauge"

var leaderRunningGaugeVec = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Subsystem: ReconcileSubSystem,
		Name:      leaderRunningCount,
		Help:      "leader controller set gauge to 1, otherwise set to 0",
	},
	[]string{"Lease"},
)

func RegisterLeaderRunningMetrics() {
	metrics.Registry.MustRegister(leaderRunningGaugeVec)
}

type LeaderRunningMetrics struct {
	Lease string
}

func (m *LeaderRunningMetrics) Lead() {
	leaderRunningGaugeVec.WithLabelValues(m.Lease).Set(1)
}

func (m *LeaderRunningMetrics) UnLead() {
	leaderRunningGaugeVec.WithLabelValues(m.Lease).Set(0)
}
