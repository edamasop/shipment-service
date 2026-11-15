package http

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
)

type ctxKey int

const (
	requestIDKey ctxKey = iota
)

const requestIDHeader = "X-Request-ID"

// RequestID returns the correlation id of the incoming request, or "" when the
// middleware is not installed.
func RequestID(ctx context.Context) string {
	id, _ := ctx.Value(requestIDKey).(string)
	return id
}

// withRequestID echoes a caller-provided X-Request-ID or generates one, so a
// single request can be followed through logs across services.
func withRequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(requestIDHeader)
		if len(id) == 0 || len(id) > 128 {
			id = newRequestID()
		}

		w.Header().Set(requestIDHeader, id)
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), requestIDKey, id)))
	})
}

func newRequestID() string {
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "unknown"
	}

	return hex.EncodeToString(buf[:])
}

// withRecovery keeps one panicking handler from taking the whole process down:
// the request is logged with a stack trace and answered with 500.
func withRecovery(log *logrus.Entry) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				recovered := recover()
				if recovered == nil {
					return
				}

				log.WithFields(logrus.Fields{
					"request_id": RequestID(r.Context()),
					"method":     r.Method,
					"path":       r.URL.Path,
					"panic":      recovered,
				}).Error("recovered from panic while handling request")

				writeJSONError(w, http.StatusInternalServerError, "internal server error")
			}()

			next.ServeHTTP(w, r)
		})
	}
}

// withCORS answers preflight requests and adds the headers configured through
// ALLOWED_ORIGINS. An empty list or "*" allows every origin, which is the
// development default.
func withCORS(allowedOrigins []string) func(http.Handler) http.Handler {
	allowAll := len(allowedOrigins) == 0
	allowed := make(map[string]struct{}, len(allowedOrigins))

	for _, origin := range allowedOrigins {
		if origin == "*" {
			allowAll = true
		}
		allowed[origin] = struct{}{}
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			origin := r.Header.Get("Origin")

			if origin != "" {
				if allowAll {
					w.Header().Set("Access-Control-Allow-Origin", "*")
				} else if _, ok := allowed[origin]; ok {
					w.Header().Set("Access-Control-Allow-Origin", origin)
					w.Header().Add("Vary", "Origin")
				}
			}

			if r.Method == http.MethodOptions && r.Header.Get("Access-Control-Request-Method") != "" {
				w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS")
				w.Header().Set("Access-Control-Allow-Headers", "Content-Type, "+requestIDHeader+", X-Signature")
				w.Header().Set("Access-Control-Max-Age", "600")
				w.WriteHeader(http.StatusNoContent)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

// withAccessLog records method, route, status and latency for every request.
// Probe and metrics endpoints are logged at debug level so they do not drown
// the business endpoints.
func withAccessLog(log *logrus.Entry) func(http.Handler) http.Handler {
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

			entry := log.WithFields(logrus.Fields{
				"request_id":  RequestID(r.Context()),
				"method":      r.Method,
				"route":       route,
				"path":        r.URL.Path,
				"status":      status,
				"bytes":       recorder.bytes,
				"duration_ms": float64(time.Since(start).Microseconds()) / 1000,
				"remote_addr": r.RemoteAddr,
			})

			switch {
			case isProbePath(r.URL.Path) && status < http.StatusBadRequest:
				entry.Debug("request completed")
			case status >= http.StatusInternalServerError:
				entry.Error("request failed")
			case status >= http.StatusBadRequest:
				entry.Warn("request rejected")
			default:
				entry.Info("request completed")
			}
		})
	}
}

func isProbePath(path string) bool {
	return strings.HasPrefix(path, "/health") ||
		strings.HasPrefix(path, "/ready") ||
		strings.HasPrefix(path, "/metrics")
}

// statusRecorder captures what the handler wrote so it can be logged and
// counted without buffering the response body.
type statusRecorder struct {
	http.ResponseWriter
	status int
	bytes  int
}

func (r *statusRecorder) WriteHeader(status int) {
	if r.status != 0 {
		return
	}

	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

func (r *statusRecorder) Write(body []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}

	n, err := r.ResponseWriter.Write(body)
	r.bytes += n

	return n, err
}

func writeJSONError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": message})
}
