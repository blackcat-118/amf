package autoscale

import (
	"context"
	"fmt"
	"math"
	"runtime"
	"sync"
	"time"

	"github.com/free5gc/amf/internal/logger"
	"github.com/free5gc/amf/internal/metrics"
	"github.com/free5gc/amf/internal/ngap"
)

// ScalingConfig holds the configuration for autoscaling decisions.
type ScalingConfig struct {
	Enabled              bool    // Enable autoscaling
	IntervalSeconds      int     // How often to run the scaling decision (seconds)
	MinWorkers           int     // Minimum number of workers
	MaxWorkers           int     // Maximum number of workers
	MinBufferSize        int     // Minimum buffer size per worker
	MaxBufferSize        int     // Maximum buffer size per worker
	WorkerCapacity       float64 // Messages per second a single worker can handle
	QueueThreshold       float64 // Scale up if avg queue depth exceeds this
	LowLoadThreshold     float64 // Scale down if rate drops below this (messages/sec)
	LowLoadDurationSec   int     // Persist low load for this many seconds before scaling down
	ScaleUpCooldownSec   int     // Wait this long after scale-up before scaling down
	ScaleDownCooldownSec int     // Wait this long after scale-down before scaling up
	PredictorMaxSamples  int     // Historical samples to keep in predictor
	PredictorEWMAAlpha   float64 // EWMA smoothing factor (0.0 to 1.0)
	PredictionHorizonSec int     // Look-ahead window for predictions (seconds)
	BufferScaleFactor    float64 // Multiplier for buffer size based on predicted queue depth
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
	cfg                  ScalingConfig
	predictor            *Predictor
	mu                   sync.Mutex
	messageCounter       uint64 // Track message count for rate calculation
	lastMessageCounter   uint64
	lastMessageCountTime time.Time
	lastScalingTime      time.Time
	lastScaleUpTime      time.Time
	lastScaleDownTime    time.Time
	lowLoadStartTime     time.Time
	lowLoadDetected      bool
	lastPredictedRate    float64
	predictionError      float64
	lastActualRate       float64
	lastMetricsCollected time.Time
}

// NewController creates a new autoscaling controller.
func NewController(cfg ScalingConfig) *Controller {
	predictor := NewPredictor(
		cfg.PredictorMaxSamples,
		cfg.PredictorEWMAAlpha,
		time.Duration(cfg.PredictionHorizonSec)*time.Second,
	)

	return &Controller{
		cfg:                  cfg,
		predictor:            predictor,
		lastScalingTime:      time.Now(),
		lastScaleUpTime:      time.Now().Add(-time.Duration(cfg.ScaleUpCooldownSec) * time.Second),
		lastScaleDownTime:    time.Now().Add(-time.Duration(cfg.ScaleDownCooldownSec) * time.Second),
		lastMessageCountTime: time.Now(),
		lastPredictedRate:    0,
		predictionError:      0,
		lastActualRate:       0,
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
	drainingQueueDepth := ngap.GetDrainingQueueDepth()
	bufferSize := ngap.GetTaskBufferSize()

	// Calculate actual message rate from dispatcher counter
	currentMessageCount := ngap.GetMessageCount()
	timeSinceLastCheck := time.Now().Sub(c.lastMessageCountTime).Seconds()
	var messageRate float64
	if timeSinceLastCheck > 0 {
		messagesDelta := currentMessageCount - c.lastMessageCounter
		messageRate = float64(messagesDelta) / timeSinceLastCheck
	}
	c.lastMessageCounter = currentMessageCount
	c.lastMessageCountTime = time.Now()
	c.lastActualRate = messageRate

	// Collect system metrics (CPU and memory)
	cpuUtil := c.getCPUUtilization()
	memUtil := c.getMemoryUtilization()

	sample := LoadSample{
		Timestamp:         time.Now(),
		MessageRate:       messageRate,
		AvgQueueDepth:     avgQueueDepth,
		MaxQueueDepth:     maxQueueDepth,
		WorkerCount:       workerCount,
		BufferSize:        bufferSize,
		CPUUtilPercent:    cpuUtil,
		MemoryUtilPercent: memUtil,
	}

	c.predictor.AddSample(sample)
	c.lastMetricsCollected = time.Now()

	// Log detailed metrics
	logger.NgapLog.Debugf("NGAP metrics: rate=%.1f msgs/sec, avgQueue=%.1f, maxQueue=%d, "+
		"drainQueue=%d, workers=%d, buffer=%d, cpu=%.1f%%, mem=%.1f%%",
		messageRate, avgQueueDepth, maxQueueDepth, drainingQueueDepth,
		workerCount, bufferSize, cpuUtil, memUtil)

	return sample
}

// getCPUUtilization returns current CPU utilization percentage.
// This is a simple implementation that tracks goroutine count as a proxy.
// For production, consider using syscall/cgroup metrics.
func (c *Controller) getCPUUtilization() float64 {
	// Simple proxy: ratio of current goroutines to worker count
	// This is a placeholder; production should use actual CPU metrics
	numGoroutines := float64(runtime.NumGoroutine())
	maxGoroutines := float64(runtime.NumCPU() * 100) // Rough estimate
	utilPercent := (numGoroutines / maxGoroutines) * 100
	if utilPercent > 100 {
		utilPercent = 100
	}
	return utilPercent
}

// getMemoryUtilization returns current memory utilization percentage.
// Uses runtime.MemStats to report heap utilization.
func (c *Controller) getMemoryUtilization() float64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// Report heap utilization as percentage of heap allocation
	// m.HeapAlloc is current allocation, m.HeapSys is total heap size
	if m.HeapSys == 0 {
		return 0
	}
	utilPercent := (float64(m.HeapAlloc) / float64(m.HeapSys)) * 100
	return utilPercent
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
	metrics.SetNgapDrainingQueueDepth(ngap.GetDrainingQueueDepth())
	metrics.SetNgapBufferSize(sample.BufferSize)
	metrics.SetNgapPredictionError(c.predictionError)
	metrics.SetNgapCPUUtilization(sample.CPUUtilPercent)
	metrics.SetNgapMemoryUtilization(sample.MemoryUtilPercent)
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
