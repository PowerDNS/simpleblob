package azure

import (
	"context"
	"errors"
	"net"
	"net/url"
	"os"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	metricLastCallTimestamp = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "storage_azure_call_timestamp_seconds",
			Help: "UNIX timestamp of last Azure API call by method",
		},
		[]string{"method"},
	)
	metricCalls = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "storage_azure_call_total",
			Help: "Azure API calls by method",
		},
		[]string{"method"},
	)
	metricCallErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "storage_azure_call_error_total",
			Help: "Azure API call errors by method",
		},
		[]string{"method"},
	)
	metricCallErrorsType = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "storage_azure_call_error_by_type_total",
			Help: "Azure API call errors by method and error type",
		},
		[]string{"method", "error"},
	)
	metricCallHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "storage_azure_call_duration_seconds",
			Help:    "Azure API call duration by method",
			Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30, 60},
		},
		[]string{"method"},
	)
)

func recordCallMetrics(method string, start time.Time) {
	metricCalls.WithLabelValues(method).Inc()
	metricLastCallTimestamp.WithLabelValues(method).SetToCurrentTime()
	metricCallHistogram.WithLabelValues(method).Observe(time.Since(start).Seconds())
}

func recordErrorMetrics(method string, err error) {
	if err == nil {
		return
	}
	errorLabel := "Unknown"
	var netErr *net.OpError
	var dnsErr *net.DNSError
	var urlErr *url.Error
	var azErr *azcore.ResponseError
	switch {
	case errors.Is(err, os.ErrNotExist):
		errorLabel = "NotFound"
	case errors.Is(err, context.DeadlineExceeded),
		(errors.As(err, &netErr) && netErr.Timeout()):
		errorLabel = "Timeout"
	case errors.As(err, &dnsErr):
		errorLabel = "DNSError"
	case errors.As(err, &urlErr):
		errorLabel = "URLError"
	case errors.As(err, &azErr):
		errorLabel = azErr.ErrorCode
	}
	metricCallErrors.WithLabelValues(method).Inc()
	metricCallErrorsType.WithLabelValues(method, errorLabel).Inc()
}

func init() {
	prometheus.MustRegister(metricLastCallTimestamp)
	prometheus.MustRegister(metricCalls)
	prometheus.MustRegister(metricCallErrors)
	prometheus.MustRegister(metricCallErrorsType)
	prometheus.MustRegister(metricCallHistogram)
}
