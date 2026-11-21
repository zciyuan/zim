// Package metrics exposes lightweight process and business metrics without
// adding another middleware dependency.
package metrics

import (
	"fmt"
	"net/http"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

// Registry holds process-local counters, gauges and latency histograms.
type Registry struct {
	mu         sync.Mutex
	counters   map[string]*atomic.Int64
	gauges     map[string]*atomic.Int64
	histograms map[string]*histogram
}

// New returns an empty registry.
func New() *Registry {
	return &Registry{
		counters:   make(map[string]*atomic.Int64),
		gauges:     make(map[string]*atomic.Int64),
		histograms: make(map[string]*histogram),
	}
}

// Default is the process-wide registry.
var Default = New()

// Inc increments a counter by one.
func (r *Registry) Inc(name string, labels map[string]string) {
	r.Add(name, labels, 1)
}

// Add increments a counter.
func (r *Registry) Add(name string, labels map[string]string, delta int64) {
	if delta == 0 {
		return
	}
	key := metricKey(name, labels)
	r.mu.Lock()
	counter := r.counters[key]
	if counter == nil {
		counter = &atomic.Int64{}
		r.counters[key] = counter
	}
	r.mu.Unlock()
	counter.Add(delta)
}

// SetGauge sets a gauge value.
func (r *Registry) SetGauge(name string, labels map[string]string, value int64) {
	key := metricKey(name, labels)
	r.mu.Lock()
	gauge := r.gauges[key]
	if gauge == nil {
		gauge = &atomic.Int64{}
		r.gauges[key] = gauge
	}
	r.mu.Unlock()
	gauge.Store(value)
}

// Observe records one latency sample in milliseconds.
func (r *Registry) Observe(name string, labels map[string]string, value int64) {
	key := metricKey(name, labels)
	r.mu.Lock()
	hist := r.histograms[key]
	if hist == nil {
		hist = &histogram{}
		r.histograms[key] = hist
	}
	r.mu.Unlock()
	hist.observe(value)
}

// Handler writes Prometheus text format plus Go runtime gauges.
func (r *Registry) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		var memory runtime.MemStats
		runtime.ReadMemStats(&memory)
		_, _ = fmt.Fprintf(w, "zim_process_goroutines %d\n", runtime.NumGoroutine())
		_, _ = fmt.Fprintf(w, "zim_process_mem_alloc_bytes %d\n", memory.Alloc)
		_, _ = fmt.Fprintf(w, "zim_process_mem_sys_bytes %d\n", memory.Sys)
		_, _ = fmt.Fprintf(w, "zim_process_gc_total %d\n", memory.NumGC)
		_, _ = fmt.Fprintf(w, "zim_process_gc_pause_ns_total %d\n", memory.PauseTotalNs)

		r.mu.Lock()
		counters := make([]string, 0, len(r.counters))
		for key := range r.counters {
			counters = append(counters, key)
		}
		gauges := make([]string, 0, len(r.gauges))
		for key := range r.gauges {
			gauges = append(gauges, key)
		}
		histograms := make([]string, 0, len(r.histograms))
		for key := range r.histograms {
			histograms = append(histograms, key)
		}
		sort.Strings(counters)
		sort.Strings(gauges)
		sort.Strings(histograms)
		for _, key := range counters {
			_, _ = fmt.Fprintf(w, "%s %d\n", key, r.counters[key].Load())
		}
		for _, key := range gauges {
			_, _ = fmt.Fprintf(w, "%s %d\n", key, r.gauges[key].Load())
		}
		for _, key := range histograms {
			hist := r.histograms[key]
			hist.mu.Lock()
			for i, bound := range latencyBounds {
				_, _ = fmt.Fprintf(w, "%s %d\n", withLE(key, bound), hist.buckets[i])
			}
			_, _ = fmt.Fprintf(w, "%s %d\n", withLE(key, 0), hist.count)
			name, labels := splitMetricKey(key)
			_, _ = fmt.Fprintf(w, "%s_sum%s %d\n", name, labels, hist.sum)
			_, _ = fmt.Fprintf(w, "%s_count%s %d\n", name, labels, hist.count)
			hist.mu.Unlock()
		}
		r.mu.Unlock()
	})
}

func metricKey(name string, labels map[string]string) string {
	if len(labels) == 0 {
		return name
	}
	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, fmt.Sprintf("%s=%q", key, labels[key]))
	}
	return name + "{" + strings.Join(parts, ",") + "}"
}

type histogram struct {
	mu      sync.Mutex
	buckets [10]int64
	count   int64
	sum     int64
}

var latencyBounds = [...]int64{5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000}

func (h *histogram) observe(value int64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.count++
	h.sum += value
	for i, bound := range latencyBounds {
		if value <= bound {
			h.buckets[i]++
		}
	}
}

// splitMetricKey splits "name{labels}" into the bare name and the label set.
func splitMetricKey(key string) (string, string) {
	index := strings.IndexByte(key, '{')
	if index < 0 {
		return key, ""
	}
	return key[:index], key[index:]
}

// withLE converts a histogram key into the Prometheus _bucket series. It keeps
// existing labels (e.g. stage="append") and prepends le.
func withLE(key string, bound int64) string {
	le := `le="+Inf"`
	if bound > 0 {
		le = fmt.Sprintf(`le="%d"`, bound)
	}
	index := strings.IndexByte(key, '{')
	if index < 0 {
		return key + "_bucket{" + le + "}"
	}
	name := key[:index]
	labels := strings.TrimSuffix(key[index+1:], "}")
	if labels == "" {
		return name + "_bucket{" + le + "}"
	}
	return name + "_bucket{" + le + "," + labels + "}"
}
