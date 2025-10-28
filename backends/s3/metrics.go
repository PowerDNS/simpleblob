package s3

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	metricLastCallTimestamp = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "storage_s3_call_timestamp_seconds",
			Help: "UNIX timestamp of last S3 API call by method",
		},
		[]string{"method"},
	)
	metricCalls = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "storage_s3_call_total",
			Help: "S3 API calls by method",
		},
		[]string{"method"},
	)
	metricCallErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "storage_s3_call_error_total",
			Help: "S3 API call errors by method",
		},
		[]string{"method"},
	)
	metricCallErrorsType = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "storage_s3_call_error_by_type_total",
			Help: "S3 API call errors by method and error type",
		},
		[]string{"method", "error"},
	)
	metricCallHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "storage_s3_call_duration_seconds",
			Help:    "S3 API call duration by method",
			Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30, 60},
		},
		[]string{"method"},
	)
)

func init() {
	prometheus.MustRegister(metricLastCallTimestamp)
	prometheus.MustRegister(metricCalls)
	prometheus.MustRegister(metricCallErrors)
	prometheus.MustRegister(metricCallErrorsType)
	prometheus.MustRegister(metricCallHistogram)
}
