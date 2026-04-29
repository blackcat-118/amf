package metrics

import (
	"github.com/free5gc/util/metrics/utils"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	SUBSYSTEM_NAME_RESOURCE = "amf_resource"
)

const (
	// NGAP Autoscaling Metrics
	NGAP_MESSAGE_RATE_GAUGE_NAME        = "ngap_message_rate_per_sec"
	NGAP_MESSAGE_RATE_GAUGE_DESC        = "Estimated NGAP message processing rate (messages/sec)"
	NGAP_PREDICTED_LOAD_GAUGE_NAME      = "ngap_predicted_load_per_sec"
	NGAP_PREDICTED_LOAD_GAUGE_DESC      = "Predicted NGAP load for next interval (messages/sec)"
	NGAP_WORKER_COUNT_GAUGE_NAME        = "ngap_worker_count"
	NGAP_WORKER_COUNT_GAUGE_DESC        = "Current number of active NGAP workers"
	NGAP_TARGET_WORKER_COUNT_GAUGE_NAME = "ngap_target_worker_count"
	NGAP_TARGET_WORKER_COUNT_GAUGE_DESC = "Target worker count based on prediction"
	NGAP_AVG_QUEUE_DEPTH_GAUGE_NAME     = "ngap_avg_queue_depth"
	NGAP_AVG_QUEUE_DEPTH_GAUGE_DESC     = "Average queue depth across NGAP workers"
	NGAP_MAX_QUEUE_DEPTH_GAUGE_NAME     = "ngap_max_queue_depth"
	NGAP_MAX_QUEUE_DEPTH_GAUGE_DESC     = "Maximum queue depth among NGAP workers"
	NGAP_BUFFER_SIZE_GAUGE_NAME         = "ngap_buffer_size"
	NGAP_BUFFER_SIZE_GAUGE_DESC         = "Task buffer size per worker"
	NGAP_SCALE_EVENTS_COUNTER_NAME      = "ngap_scale_events_total"
	NGAP_SCALE_EVENTS_COUNTER_DESC      = "Total number of scale-up and scale-down events"
	NGAP_PREDICTION_ERROR_GAUGE_NAME    = "ngap_prediction_error_percent"
	NGAP_PREDICTION_ERROR_GAUGE_DESC    = "Percentage error of load prediction vs actual (absolute)"
)

const (
	// Label names
	SCALE_ACTION_LABEL = "action"
	REASON_LABEL       = "reason"

	// Label values
	SCALE_UP_ACTION   = "scale_up"
	SCALE_DOWN_ACTION = "scale_down"
)

var (
	// NGAP Autoscaling Metrics
	ngapMessageRateGauge       prometheus.Gauge
	ngapPredictedLoadGauge     prometheus.Gauge
	ngapWorkerCountGauge       prometheus.Gauge
	ngapTargetWorkerCountGauge prometheus.Gauge
	ngapAvgQueueDepthGauge     prometheus.Gauge
	ngapMaxQueueDepthGauge     prometheus.Gauge
	ngapBufferSizeGauge        prometheus.Gauge
	ngapScaleEventsCounter     *prometheus.CounterVec
	ngapPredictionErrorGauge   prometheus.Gauge
)

func GetResourceMetrics(namespace string) []prometheus.Collector {
	var collectors []prometheus.Collector

	// Message rate gauge
	ngapMessageRateGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: SUBSYSTEM_NAME_RESOURCE,
			Name:      NGAP_MESSAGE_RATE_GAUGE_NAME,
			Help:      NGAP_MESSAGE_RATE_GAUGE_DESC,
		},
	)
	ngapMessageRateGauge.Set(0)
	collectors = append(collectors, ngapMessageRateGauge)

	// Predicted load gauge
	ngapPredictedLoadGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: SUBSYSTEM_NAME_RESOURCE,
			Name:      NGAP_PREDICTED_LOAD_GAUGE_NAME,
			Help:      NGAP_PREDICTED_LOAD_GAUGE_DESC,
		},
	)
	ngapPredictedLoadGauge.Set(0)
	collectors = append(collectors, ngapPredictedLoadGauge)

	// Worker count gauge
	ngapWorkerCountGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: SUBSYSTEM_NAME_RESOURCE,
			Name:      NGAP_WORKER_COUNT_GAUGE_NAME,
			Help:      NGAP_WORKER_COUNT_GAUGE_DESC,
		},
	)
	ngapWorkerCountGauge.Set(0)
	collectors = append(collectors, ngapWorkerCountGauge)

	// Target worker count gauge
	ngapTargetWorkerCountGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: SUBSYSTEM_NAME_RESOURCE,
			Name:      NGAP_TARGET_WORKER_COUNT_GAUGE_NAME,
			Help:      NGAP_TARGET_WORKER_COUNT_GAUGE_DESC,
		},
	)
	ngapTargetWorkerCountGauge.Set(0)
	collectors = append(collectors, ngapTargetWorkerCountGauge)

	// Average queue depth gauge
	ngapAvgQueueDepthGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: SUBSYSTEM_NAME_RESOURCE,
			Name:      NGAP_AVG_QUEUE_DEPTH_GAUGE_NAME,
			Help:      NGAP_AVG_QUEUE_DEPTH_GAUGE_DESC,
		},
	)
	ngapAvgQueueDepthGauge.Set(0)
	collectors = append(collectors, ngapAvgQueueDepthGauge)

	// Max queue depth gauge
	ngapMaxQueueDepthGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: SUBSYSTEM_NAME_RESOURCE,
			Name:      NGAP_MAX_QUEUE_DEPTH_GAUGE_NAME,
			Help:      NGAP_MAX_QUEUE_DEPTH_GAUGE_DESC,
		},
	)
	ngapMaxQueueDepthGauge.Set(0)
	collectors = append(collectors, ngapMaxQueueDepthGauge)

	// Buffer size gauge
	ngapBufferSizeGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: SUBSYSTEM_NAME_RESOURCE,
			Name:      NGAP_BUFFER_SIZE_GAUGE_NAME,
			Help:      NGAP_BUFFER_SIZE_GAUGE_DESC,
		},
	)
	ngapBufferSizeGauge.Set(0)
	collectors = append(collectors, ngapBufferSizeGauge)

	// Scale events counter
	ngapScaleEventsCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: SUBSYSTEM_NAME_RESOURCE,
			Name:      NGAP_SCALE_EVENTS_COUNTER_NAME,
			Help:      NGAP_SCALE_EVENTS_COUNTER_DESC,
		},
		[]string{SCALE_ACTION_LABEL, REASON_LABEL},
	)
	collectors = append(collectors, ngapScaleEventsCounter)

	// Prediction error gauge
	ngapPredictionErrorGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: SUBSYSTEM_NAME_RESOURCE,
			Name:      NGAP_PREDICTION_ERROR_GAUGE_NAME,
			Help:      NGAP_PREDICTION_ERROR_GAUGE_DESC,
		},
	)
	ngapPredictionErrorGauge.Set(0)
	collectors = append(collectors, ngapPredictionErrorGauge)

	return collectors
}

// Update metrics functions
func SetNgapMessageRate(rate float64) {
	if utils.IsBusinessMetricsEnabled() && ngapMessageRateGauge != nil {
		ngapMessageRateGauge.Set(rate)
	}
}

func SetNgapPredictedLoad(load float64) {
	if utils.IsBusinessMetricsEnabled() && ngapPredictedLoadGauge != nil {
		ngapPredictedLoadGauge.Set(load)
	}
}

func SetNgapWorkerCount(count int) {
	if utils.IsBusinessMetricsEnabled() && ngapWorkerCountGauge != nil {
		ngapWorkerCountGauge.Set(float64(count))
	}
}

func SetNgapTargetWorkerCount(count int) {
	if utils.IsBusinessMetricsEnabled() && ngapTargetWorkerCountGauge != nil {
		ngapTargetWorkerCountGauge.Set(float64(count))
	}
}

func SetNgapAvgQueueDepth(depth float64) {
	if utils.IsBusinessMetricsEnabled() && ngapAvgQueueDepthGauge != nil {
		ngapAvgQueueDepthGauge.Set(depth)
	}
}

func SetNgapMaxQueueDepth(depth int) {
	if utils.IsBusinessMetricsEnabled() && ngapMaxQueueDepthGauge != nil {
		ngapMaxQueueDepthGauge.Set(float64(depth))
	}
}

func SetNgapBufferSize(size int) {
	if utils.IsBusinessMetricsEnabled() && ngapBufferSizeGauge != nil {
		ngapBufferSizeGauge.Set(float64(size))
	}
}

func IncNgapScaleEvents(action, reason string) {
	if utils.IsBusinessMetricsEnabled() && ngapScaleEventsCounter != nil {
		ngapScaleEventsCounter.With(prometheus.Labels{
			SCALE_ACTION_LABEL: action,
			REASON_LABEL:       reason,
		}).Inc()
	}
}

func SetNgapPredictionError(errorPercent float64) {
	if utils.IsBusinessMetricsEnabled() && ngapPredictionErrorGauge != nil {
		ngapPredictionErrorGauge.Set(errorPercent)
	}
}
