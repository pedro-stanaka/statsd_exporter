package httputil

import (
	"fmt"
	"net/http"

	"github.com/prometheus/statsd_exporter/pkg/level"

	"github.com/go-kit/log"
)

// RequestHeadersLogging is a middleware that logs the request headers and path.
func RequestHeadersLogging(logger log.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			level.Info(logger).Log("msg", "Request received", "headers", fmt.Sprint(r.Header), "path", r.URL.Path, "remote-address", r.RemoteAddr)
			next.ServeHTTP(w, r)
		})
	}
}
