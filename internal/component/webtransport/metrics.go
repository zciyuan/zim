package webtransport

import "github.com/zciyuan/ZIM/internal/component/metrics"

const (
	metricSessions = "zim_gate_webtransport_sessions_total"
	metricActive   = "zim_gate_webtransport_sessions_active"
)

// observeSession records one session admission or rejection.
func observeSession(result string) {
	metrics.Default.Inc(metricSessions, map[string]string{"result": result})
}

// observeActive records the current number of accepted sessions.
func observeActive(active int) {
	metrics.Default.SetGauge(metricActive, nil, int64(active))
}
