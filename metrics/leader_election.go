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
