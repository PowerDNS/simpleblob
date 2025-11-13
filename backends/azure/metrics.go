package azure

import (
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
)

func init() {
	prometheus.MustRegister(metricLastCallTimestamp)
	prometheus.MustRegister(metricCalls)
	prometheus.MustRegister(metricCallErrors)
}
