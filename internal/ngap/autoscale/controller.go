package autoscale

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/free5gc/amf/internal/logger"
	"github.com/free5gc/amf/internal/metrics"
	"github.com/free5gc/amf/internal/ngap"
)

// ScalingConfig holds the configuration for autoscaling decisions.
type ScalingConfig struct {
	Enabled              bool          // Enable autoscaling
	IntervalSeconds      int           // How often to run the scaling decision (seconds)
	MinWorkers           int           // Minimum number of workers
	MaxWorkers           int           // Maximum number of workers
	MinBufferSize        int           // Minimum buffer size per worker
	MaxBufferSize        int           // Maximum buffer size per worker
	WorkerCapacity       float64       // Messages per second a single worker can handle
	QueueThreshold       float64       // Scale up if avg queue depth exceeds this
	LowLoadThreshold     float64       // Scale down if rate drops below this (messages/sec)
	LowLoadDurationSec   int           // Persist low load for this many seconds before scaling down
	ScaleUpCooldownSec   int           // Wait this long after scale-up before scaling down
	ScaleDownCooldownSec int           // Wait this long after scale-down before scaling up
	PredictorMaxSamples  int           // Historical samples to keep in predictor
	PredictorEWMAAlpha   float64       // EWMA smoothing factor (0.0 to 1.0)
	PredictionHorizonSec int           // Look-ahead window for predictions (seconds)
	BufferScaleFactor    float64       // Multiplier for buffer size based on predicted queue depth
}

// DefaultScalingConfig returns a reasonable default configuration.
func DefaultScalingConfig() ScalingConfig {
	return ScalingConfig{
		Enabled:              true,
		IntervalSeconds:      5,
		MinWorkers:           2,
		MaxWorkers:           128,
		MinBufferSize:        256,
		MaxBufferSize:        8192,
		WorkerCapacity:       1000.0, // 1000 messages/sec per worker
		QueueThreshold:       100.0,  // Scale up if avg queue > 100
		LowLoadThreshold:     100.0,  // Scale down if rate < 100 msgs/sec
		LowLoadDurationSec:   60,
		ScaleUpCooldownSec:   10,
		ScaleDownCooldownSec: 30,
		PredictorMaxSamples:  60,
		PredictorEWMAAlpha:   0.2,
		PredictionHorizonSec: 30,
		BufferScaleFactor:    1.5, // buffer size = predicted_rate / worker_capacity * buffer_scale_factor
	}
}

// Controller manages NGAP resource autoscaling.
type Controller struct {
	cfg                   ScalingConfig
	predictor             *Predictor
	mu                    sync.Mutex
	messageCounter        uint64  // Track message count for rate calculation
	lastMessageCounter    uint64
	lastScalingTime       time.Time
	lastScaleUpTime       time.Time
	lastScaleDownTime     time.Time
	lowLoadStartTime      time.Time
	lowLoadDetected       bool
	lastPredictedRate     float64
	predictionError       float64
	lastActualRate        float64
	lastMetricsCollected  time.Time
}

// NewController creates a new autoscaling controller.
func NewController(cfg ScalingConfig) *Controller {
	predictor := NewPredictor(
		cfg.PredictorMaxSamples,
		cfg.PredictorEWMAAlpha,
		time.Duration(cfg.PredictionHorizonSec)*time.Second,
	)

	return &Controller{
		cfg:                cfg,
		predictor:          predictor,
		lastScalingTime:    time.Now(),
		lastScaleUpTime:    time.Now().Add(-time.Duration(cfg.ScaleUpCooldownSec) * time.Second),
		lastScaleDownTime:  time.Now().Add(-time.Duration(cfg.ScaleDownCooldownSec) * time.Second),
		lastPredictedRate:  0,
		predictionError:    0,
		lastActualRate:     0,
		lastMetricsCollected: time.Now(),
	}
}

// CollectMetrics gathers current NGAP load metrics and adds them to the predictor.
func (c *Controller) CollectMetrics() LoadSample {
	c.mu.Lock()
	defer c.mu.Unlock()

	workerCount := ngap.GetWorkerCount()
	avgQueueDepth := ngap.GetAverageQueueDepth()
	maxQueueDepth := ngap.GetMaxQueueDepth()
	totalQueueDepth := ngap.GetTotalQueueDepth()
	bufferSize := ngap.GetTaskBufferSize()

	// Estimate message rate from total queue depth as a proxy
	// In real scenario, you'd instrument the task dispatch to count messages
	messageRate := c.estimateMessageRate(totalQueueDepth, workerCount)
	c.lastActualRate = messageRate

	sample := LoadSample{
		Timestamp:      time.Now(),
		MessageRate:    messageRate,
		AvgQueueDepth:  avgQueueDepth,
		MaxQueueDepth:  maxQueueDepth,
		WorkerCount:    workerCount,
		BufferSize:     bufferSize,
		CPUUtilPercent: 0, // TODO: add system CPU monitoring
	}

	c.predictor.AddSample(sample)
	c.lastMetricsCollected = time.Now()

	return sample
}

// estimateMessageRate is a placeholder. In production, instrument the dispatcher
// to count actual messages. For now, use a simple heuristic based on queue depth.
func (c *Controller) estimateMessageRate(totalQueueDepth int, workerCount int) float64 {
	// Simple heuristic: if queue is growing, estimate rate from queue depth delta
	// This is a placeholder; real implementation should count dispatched messages
	if workerCount == 0 {
		return 0
	}

	// Assume queue depth grows by ~1-2 per new message under load
	// This is rough; actual rate should be instrumented
	rate := float64(totalQueueDepth) / float64(workerCount)
	if rate < 0 {
		rate = 0
	}
	return rate
}

// DecideScaling determines whether to scale workers and buffer size based on current state.
// Returns (shouldScale, targetWorkers, targetBufferSize, reason).
func (c *Controller) DecideScaling() (bool, int, int, string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	currentWorkers := ngap.GetWorkerCount()
	avgQueueDepth := ngap.GetAverageQueueDepth()
	predictedRate := c.predictor.PredictMessageRate()
	c.lastPredictedRate = predictedRate

	// Calculate prediction error
	if c.lastActualRate > 0 {
		c.predictionError = math.Abs(predictedRate-c.lastActualRate) / c.lastActualRate * 100
	} else {
		c.predictionError = 0
	}

	// Determine target worker count based on predicted load
	targetWorkers := int(math.Ceil(predictedRate / c.cfg.WorkerCapacity))
	if targetWorkers < c.cfg.MinWorkers {
		targetWorkers = c.cfg.MinWorkers
	}
	if targetWorkers > c.cfg.MaxWorkers {
		targetWorkers = c.cfg.MaxWorkers
	}

	// Determine target buffer size
	targetBufferSize := int(math.Ceil(predictedRate / c.cfg.WorkerCapacity * c.cfg.BufferScaleFactor * 100))
	if targetBufferSize < c.cfg.MinBufferSize {
		targetBufferSize = c.cfg.MinBufferSize
	}
	if targetBufferSize > c.cfg.MaxBufferSize {
		targetBufferSize = c.cfg.MaxBufferSize
	}

	// Check for scale-up condition: high queue depth or predicted rate exceeds capacity
	if avgQueueDepth > c.cfg.QueueThreshold && targetWorkers > currentWorkers {
		if now.Sub(c.lastScaleUpTime) > time.Duration(c.cfg.ScaleUpCooldownSec)*time.Second {
			reason := fmt.Sprintf("high_queue_depth=%.1f,predicted_rate=%.1f", avgQueueDepth, predictedRate)
			return true, targetWorkers, targetBufferSize, reason
		}
	}

	// Check for scale-down condition: sustained low load
	if predictedRate < c.cfg.LowLoadThreshold && targetWorkers < currentWorkers {
		if !c.lowLoadDetected {
			c.lowLoadStartTime = now
			c.lowLoadDetected = true
			logger.NgapLog.Infof("Low load detected, starting countdown: rate=%.1f", predictedRate)
		}

		lowLoadDuration := now.Sub(c.lowLoadStartTime)
		if lowLoadDuration > time.Duration(c.cfg.LowLoadDurationSec)*time.Second {
			if now.Sub(c.lastScaleDownTime) > time.Duration(c.cfg.ScaleDownCooldownSec)*time.Second {
				reason := fmt.Sprintf("sustained_low_load=%.1f,duration=%d", predictedRate, int(lowLoadDuration.Seconds()))
				c.lowLoadDetected = false
				return true, targetWorkers, targetBufferSize, reason
			}
		}
	} else {
		// Reset low-load detection if load increased
		c.lowLoadDetected = false
	}

	return false, currentWorkers, c.cfg.MinBufferSize, "no_scaling_needed"
}

// Run starts the autoscaling control loop.
func (c *Controller) Run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	if !c.cfg.Enabled {
		logger.NgapLog.Infof("NGAP autoscaling is disabled")
		return
	}

	ticker := time.NewTicker(time.Duration(c.cfg.IntervalSeconds) * time.Second)
	defer ticker.Stop()

	logger.NgapLog.Infof("NGAP autoscaling controller started (interval=%ds, min_workers=%d, max_workers=%d)",
		c.cfg.IntervalSeconds, c.cfg.MinWorkers, c.cfg.MaxWorkers)

	for {
		select {
		case <-ctx.Done():
			logger.NgapLog.Infof("NGAP autoscaling controller stopping")
			return
		case <-ticker.C:
			c.runControlCycle()
		}
	}
}

// runControlCycle executes one autoscaling decision cycle.
func (c *Controller) runControlCycle() {
	// Collect current metrics
	sample := c.CollectMetrics()

	// Decide if scaling is needed
	shouldScale, targetWorkers, targetBufferSize, reason := c.DecideScaling()

	// Export metrics
	c.exportMetrics(sample, targetWorkers)

	if shouldScale {
		currentWorkers := ngap.GetWorkerCount()
		logger.NgapLog.Infof("NGAP autoscaling decision: scale from %d to %d workers, buffer=%d, reason=%s",
			currentWorkers, targetWorkers, targetBufferSize, reason)

		if targetWorkers > currentWorkers {
			c.mu.Lock()
			c.lastScaleUpTime = time.Now()
			c.mu.Unlock()
			metrics.IncNgapScaleEvents(metrics.SCALE_UP_ACTION, reason)
		} else if targetWorkers < currentWorkers {
			c.mu.Lock()
			c.lastScaleDownTime = time.Now()
			c.mu.Unlock()
			metrics.IncNgapScaleEvents(metrics.SCALE_DOWN_ACTION, reason)
		}

		// Execute scaling in NGAP scheduler
		if err := ngap.ScaleWorkers(targetWorkers, targetBufferSize); err != nil {
			logger.NgapLog.Errorf("NGAP autoscaling failed: %v", err)
		}
	}
}

// exportMetrics publishes autoscaling metrics to Prometheus.
func (c *Controller) exportMetrics(sample LoadSample, targetWorkers int) {
	metrics.SetNgapMessageRate(sample.MessageRate)
	metrics.SetNgapPredictedLoad(c.lastPredictedRate)
	metrics.SetNgapWorkerCount(sample.WorkerCount)
	metrics.SetNgapTargetWorkerCount(targetWorkers)
	metrics.SetNgapAvgQueueDepth(sample.AvgQueueDepth)
	metrics.SetNgapMaxQueueDepth(sample.MaxQueueDepth)
	metrics.SetNgapBufferSize(sample.BufferSize)
	metrics.SetNgapPredictionError(c.predictionError)
}

// GetPredictionError returns the current prediction error percentage.
func (c *Controller) GetPredictionError() float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.predictionError
}

// GetLastMetrics returns the last collected metrics snapshot.
func (c *Controller) GetLastMetrics() LoadSample {
	c.mu.Lock()
	defer c.mu.Unlock()

	samples := c.predictor.GetRecentSamples(1)
	if len(samples) > 0 {
		return samples[0]
	}
	return LoadSample{}
}
