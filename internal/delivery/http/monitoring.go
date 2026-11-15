package http

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/edamasop/messaging"
	"github.com/sirupsen/logrus"
)

// Pinger is the small part of pgxpool.Pool the readiness check needs.
type Pinger interface {
	Ping(ctx context.Context) error
}

// OutboxStats reports the outbox backlog (pending and parked rows).
type OutboxStats interface {
	Snapshot(ctx context.Context) (pending int64, parked int64, err error)
}

// MonitoringDeps is everything the probe and metrics endpoints need. Every
// field except Service and Log is optional, which keeps a partially configured
// service observable instead of panicking on a nil dependency.
type MonitoringDeps struct {
	Service  string
	DB       Pinger
	Consumer messaging.Consumer
	Producer messaging.Producer
	Outbox   OutboxStats
	Log      *logrus.Entry
	Started  time.Time
}

type monitoringHandler struct {
	deps MonitoringDeps
	http *httpMetrics
}

type readinessCheck struct {
	OK      bool   `json:"ok"`
	Details string `json:"details,omitempty"`
}

type readinessResponse struct {
	Service       string                    `json:"service"`
	Status        string                    `json:"status"`
	UptimeSeconds float64                   `json:"uptime_seconds"`
	Checks        map[string]readinessCheck `json:"checks"`
}

// registerMonitoring wires the liveness, readiness and metrics endpoints.
func registerMonitoring(mux *http.ServeMux, deps MonitoringDeps, httpStats *httpMetrics) {
	handler := &monitoringHandler{deps: deps, http: httpStats}

	mux.HandleFunc("GET /health", handler.health)
	mux.HandleFunc("GET /ready", handler.ready)
	mux.HandleFunc("GET /metrics", handler.metrics)
}

// health is a liveness probe: if the process can answer, it is alive. It
// deliberately does not touch the database or Kafka, otherwise a dependency
// outage would restart a perfectly healthy process.
func (h *monitoringHandler) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"service":        h.deps.Service,
		"status":         "ok",
		"uptime_seconds": time.Since(h.deps.Started).Seconds(),
	})
}

// ready is a readiness probe: the service only takes traffic when it can
// actually do its job, i.e. the database is reachable and the consumer has no
// sustained failure rate. Kafka producer health is reported but does not fail
// the probe: orders are buffered in the outbox while the broker is away.
func (h *monitoringHandler) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()

	checks := make(map[string]readinessCheck, 4)
	ready := true

	if h.deps.DB != nil {
		if err := h.deps.DB.Ping(ctx); err != nil {
			checks["database"] = readinessCheck{OK: false, Details: err.Error()}
			ready = false
		} else {
			checks["database"] = readinessCheck{OK: true}
		}
	}

	if h.deps.Consumer != nil {
		health := h.deps.Consumer.GetHealth()
		checks["kafka_consumer"] = readinessCheck{
			OK:      health.IsHealthy,
			Details: brokerDetails(health.IsHealthy, health.LastError),
		}
		if !health.IsHealthy {
			ready = false
		}
	}

	if h.deps.Producer != nil {
		health := h.deps.Producer.Health()
		// Informational only: the outbox survives a producer outage.
		checks["kafka_producer"] = readinessCheck{
			OK:      true,
			Details: brokerDetails(health.IsHealthy, health.LastError),
		}
	}

	if h.deps.Outbox != nil {
		pending, parked, err := h.deps.Outbox.Snapshot(ctx)
		switch {
		case err != nil:
			checks["outbox"] = readinessCheck{OK: false, Details: err.Error()}
			ready = false
		default:
			checks["outbox"] = readinessCheck{
				OK:      true,
				Details: fmt.Sprintf("pending=%d parked=%d", pending, parked),
			}
		}
	}

	status := http.StatusOK
	label := "ready"
	if !ready {
		status = http.StatusServiceUnavailable
		label = "not_ready"
	}

	writeJSON(w, status, readinessResponse{
		Service:       h.deps.Service,
		Status:        label,
		UptimeSeconds: time.Since(h.deps.Started).Seconds(),
		Checks:        checks,
	})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func brokerDetails(healthy bool, lastErr string) string {
	if healthy {
		return "ok"
	}
	if lastErr == "" {
		return "unhealthy"
	}

	return "unhealthy: " + lastErr
}

// metrics renders the Prometheus text exposition format. The client library is
// intentionally not used: the counters we need already live in the messaging
// package, and a hand-written exposition keeps the dependency set small.
func (h *monitoringHandler) metrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")

	buf := bufio.NewWriter(w)
	defer buf.Flush()

	service := labelValue(h.deps.Service)

	fmt.Fprintf(buf, "# HELP edamsop_service_up Always 1 while the process is running.\n")
	fmt.Fprintf(buf, "# TYPE edamsop_service_up gauge\n")
	fmt.Fprintf(buf, "edamsop_service_up{service=\"%s\"} 1\n", service)

	fmt.Fprintf(buf, "# HELP edamsop_uptime_seconds Seconds since the process started.\n")
	fmt.Fprintf(buf, "# TYPE edamsop_uptime_seconds gauge\n")
	fmt.Fprintf(buf, "edamsop_uptime_seconds{service=\"%s\"} %s\n", service, formatFloat(time.Since(h.deps.Started).Seconds()))

	if h.deps.Consumer != nil {
		health := h.deps.Consumer.GetHealth()

		fmt.Fprintf(buf, "# TYPE edamsop_kafka_consumer_healthy gauge\n")
		fmt.Fprintf(buf, "edamsop_kafka_consumer_healthy{service=\"%s\"} %d\n", service, boolToInt(health.IsHealthy))

		fmt.Fprintf(buf, "# TYPE edamsop_kafka_consumer_messages_total counter\n")
		fmt.Fprintf(buf, "edamsop_kafka_consumer_messages_total{service=\"%s\",result=\"processed\"} %d\n", service, health.MessagesProcessed)
		fmt.Fprintf(buf, "edamsop_kafka_consumer_messages_total{service=\"%s\",result=\"failed\"} %d\n", service, health.MessagesFailed)
		fmt.Fprintf(buf, "edamsop_kafka_consumer_messages_total{service=\"%s\",result=\"retried\"} %d\n", service, health.MessagesRetried)
		fmt.Fprintf(buf, "edamsop_kafka_consumer_messages_total{service=\"%s\",result=\"dlq\"} %d\n", service, health.MessagesDLQ)
	}

	if h.deps.Producer != nil {
		health := h.deps.Producer.Health()

		fmt.Fprintf(buf, "# TYPE edamsop_kafka_producer_healthy gauge\n")
		fmt.Fprintf(buf, "edamsop_kafka_producer_healthy{service=\"%s\"} %d\n", service, boolToInt(health.IsHealthy))

		fmt.Fprintf(buf, "# TYPE edamsop_kafka_producer_messages_total counter\n")
		fmt.Fprintf(buf, "edamsop_kafka_producer_messages_total{service=\"%s\",result=\"sent\"} %d\n", service, health.MessagesSent)
		fmt.Fprintf(buf, "edamsop_kafka_producer_messages_total{service=\"%s\",result=\"failed\"} %d\n", service, health.MessagesFailed)

		fmt.Fprintf(buf, "# TYPE edamsop_kafka_producer_async_queue gauge\n")
		fmt.Fprintf(buf, "edamsop_kafka_producer_async_queue{service=\"%s\"} %d\n", service, health.AsyncQueueLength)
	}

	if h.deps.Outbox != nil {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		pending, parked, err := h.deps.Outbox.Snapshot(ctx)
		cancel()

		if err != nil {
			h.deps.Log.WithError(err).Warn("failed to read outbox backlog for metrics")
		} else {
			fmt.Fprintf(buf, "# HELP edamsop_outbox_pending Events waiting to be published.\n")
			fmt.Fprintf(buf, "# TYPE edamsop_outbox_pending gauge\n")
			fmt.Fprintf(buf, "edamsop_outbox_pending{service=\"%s\"} %d\n", service, pending)

			fmt.Fprintf(buf, "# HELP edamsop_outbox_parked Events that exhausted their publish attempts.\n")
			fmt.Fprintf(buf, "# TYPE edamsop_outbox_parked gauge\n")
			fmt.Fprintf(buf, "edamsop_outbox_parked{service=\"%s\"} %d\n", service, parked)
		}
	}

	h.http.write(buf, service)
}

// httpMetrics aggregates request counters per method/route/status. The route is
// the matched mux pattern (for example "/v1/orders/{id}"), not the raw path, so
// the cardinality stays bounded.
type httpMetrics struct {
	mu       sync.Mutex
	requests map[httpKey]int64
	duration map[httpKey]float64
}

type httpKey struct {
	method string
	route  string
	status int
}

func newHTTPMetrics() *httpMetrics {
	return &httpMetrics{
		requests: make(map[httpKey]int64),
		duration: make(map[httpKey]float64),
	}
}

func (m *httpMetrics) observe(method, route string, status int, duration time.Duration) {
	key := httpKey{method: method, route: route, status: status}

	m.mu.Lock()
	m.requests[key]++
	m.duration[key] += duration.Seconds()
	m.mu.Unlock()
}

func (m *httpMetrics) write(buf *bufio.Writer, service string) {
	m.mu.Lock()
	keys := make([]httpKey, 0, len(m.requests))
	for key := range m.requests {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].route != keys[j].route {
			return keys[i].route < keys[j].route
		}
		if keys[i].method != keys[j].method {
			return keys[i].method < keys[j].method
		}
		return keys[i].status < keys[j].status
	})

	type snapshot struct {
		key      httpKey
		count    int64
		duration float64
	}
	rows := make([]snapshot, 0, len(keys))
	for _, key := range keys {
		rows = append(rows, snapshot{key: key, count: m.requests[key], duration: m.duration[key]})
	}
	m.mu.Unlock()

	fmt.Fprintf(buf, "# HELP edamsop_http_requests_total Requests handled by the HTTP server.\n")
	fmt.Fprintf(buf, "# TYPE edamsop_http_requests_total counter\n")
	for _, row := range rows {
		fmt.Fprintf(buf, "edamsop_http_requests_total{service=\"%s\",method=\"%s\",route=\"%s\",status=\"%d\"} %d\n",
			service, labelValue(row.key.method), labelValue(row.key.route), row.key.status, row.count)
	}

	fmt.Fprintf(buf, "# HELP edamsop_http_request_duration_seconds_sum Total time spent handling requests.\n")
	fmt.Fprintf(buf, "# TYPE edamsop_http_request_duration_seconds_sum counter\n")
	for _, row := range rows {
		fmt.Fprintf(buf, "edamsop_http_request_duration_seconds_sum{service=\"%s\",method=\"%s\",route=\"%s\",status=\"%d\"} %s\n",
			service, labelValue(row.key.method), labelValue(row.key.route), row.key.status, formatFloat(row.duration))
	}

	fmt.Fprintf(buf, "# HELP edamsop_http_request_duration_seconds_count Number of timed requests.\n")
	fmt.Fprintf(buf, "# TYPE edamsop_http_request_duration_seconds_count counter\n")
	for _, row := range rows {
		fmt.Fprintf(buf, "edamsop_http_request_duration_seconds_count{service=\"%s\",method=\"%s\",route=\"%s\",status=\"%d\"} %d\n",
			service, labelValue(row.key.method), labelValue(row.key.route), row.key.status, row.count)
	}
}

// withMetrics feeds the HTTP counters. It runs inside the access-log
// middleware, so a panicking handler is still counted when recovery answers.
func withMetrics(metrics *httpMetrics) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			recorder := &statusRecorder{ResponseWriter: w}

			next.ServeHTTP(recorder, r)

			status := recorder.status
			if status == 0 {
				status = http.StatusOK
			}

			route := r.Pattern
			if route == "" {
				route = "unmatched"
			}

			metrics.observe(r.Method, route, status, time.Since(start))
		})
	}
}

func boolToInt(value bool) int {
	if value {
		return 1
	}

	return 0
}

func formatFloat(value float64) string {
	return strconv.FormatFloat(value, 'f', -1, 64)
}

// labelValue escapes the characters that would otherwise break the exposition
// format.
func labelValue(value string) string {
	escaped := make([]rune, 0, len(value))
	for _, char := range value {
		switch char {
		case '\\':
			escaped = append(escaped, '\\', '\\')
		case '"':
			escaped = append(escaped, '\\', '"')
		case '\n':
			escaped = append(escaped, '\\', 'n')
		default:
			escaped = append(escaped, char)
		}
	}

	return string(escaped)
}
