package metrics

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRegistryHandler(t *testing.T) {
	registry := New()
	registry.Inc("zim_test_total", map[string]string{"result": "ok"})
	registry.Add("zim_test_total", map[string]string{"result": "ok"}, 2)
	registry.SetGauge("zim_test_gauge", nil, 7)
	registry.Observe("zim_test_latency_ms", nil, 42)
	registry.Observe("zim_test_labeled_ms", map[string]string{"stage": "append"}, 42)
	recorder := httptest.NewRecorder()
	registry.Handler().ServeHTTP(recorder, httptest.NewRequest("GET", "/metrics", nil))
	body := recorder.Body.String()
	for _, want := range []string{
		"zim_test_total{result=\"ok\"} 3",
		"zim_test_gauge 7",
		"zim_test_latency_ms_bucket{le=\"50\"} 1",
		"zim_test_latency_ms_count 1",
		`zim_test_labeled_ms_bucket{le="50",stage="append"} 1`,
		`zim_test_labeled_ms_sum{stage="append"} 42`,
		`zim_test_labeled_ms_count{stage="append"} 1`,
		"zim_process_goroutines",
		"zim_process_gc_total",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("missing %q in %s", want, body)
		}
	}
}
