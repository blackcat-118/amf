package autoscale

import (
	"math"
	"sync"
	"time"
)

// LoadSample represents a snapshot of NGAP metrics at a point in time.
type LoadSample struct {
	Timestamp      time.Time
	MessageRate    float64 // messages per second
	AvgQueueDepth  float64
	MaxQueueDepth  int
	WorkerCount    int
	BufferSize     int
	CPUUtilPercent float64
}

// Predictor forecasts future NGAP load based on recent samples.
type Predictor struct {
	mu              sync.RWMutex
	samples         []LoadSample
	maxSamples      int
	ewmaAlpha       float64 // smoothing factor for EWMA (0.0 to 1.0)
	ewmaRate        float64 // current EWMA estimate of message rate
	predictionHorizon time.Duration // how far ahead to predict
}

// NewPredictor creates a new load predictor.
// maxSamples: how many historical samples to keep (e.g., 60 for 1-minute history)
// ewmaAlpha: smoothing factor for EWMA (e.g., 0.2 for recent data weighting)
// horizon: prediction window (e.g., 30 seconds ahead)
func NewPredictor(maxSamples int, ewmaAlpha float64, horizon time.Duration) *Predictor {
	if maxSamples <= 0 {
		maxSamples = 60
	}
	if ewmaAlpha < 0 || ewmaAlpha > 1 {
		ewmaAlpha = 0.2
	}
	if horizon <= 0 {
		horizon = 30 * time.Second
	}

	return &Predictor{
		samples:           make([]LoadSample, 0, maxSamples),
		maxSamples:        maxSamples,
		ewmaAlpha:         ewmaAlpha,
		ewmaRate:          0,
		predictionHorizon: horizon,
	}
}

// AddSample adds a new load sample to the predictor.
func (p *Predictor) AddSample(sample LoadSample) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Initialize EWMA on first sample
	if len(p.samples) == 0 {
		p.ewmaRate = sample.MessageRate
	} else {
		// Update EWMA: new_ewma = alpha * new_value + (1 - alpha) * old_ewma
		p.ewmaRate = p.ewmaAlpha*sample.MessageRate + (1-p.ewmaAlpha)*p.ewmaRate
	}

	// Add sample to history
	p.samples = append(p.samples, sample)

	// Trim old samples if exceeds max
	if len(p.samples) > p.maxSamples {
		p.samples = p.samples[1:]
	}
}

// PredictMessageRate predicts the average NGAP message rate over the next prediction horizon.
// Uses EWMA as primary method, falls back to moving average if needed.
func (p *Predictor) PredictMessageRate() float64 {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if len(p.samples) == 0 {
		return 0
	}

	// Primary: use EWMA estimate
	if len(p.samples) >= 2 {
		return p.ewmaRate
	}

	// Fallback: return last observed rate
	return p.samples[len(p.samples)-1].MessageRate
}

// PredictMessageRateMovingAverage predicts using a simple moving average over the last N samples.
func (p *Predictor) PredictMessageRateMovingAverage(window int) float64 {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if len(p.samples) == 0 {
		return 0
	}

	if window <= 0 || window > len(p.samples) {
		window = len(p.samples)
	}

	sum := 0.0
	for i := len(p.samples) - window; i < len(p.samples); i++ {
		sum += p.samples[i].MessageRate
	}
	return sum / float64(window)
}

// PredictMessageRateLinearRegression predicts using linear regression over recent samples.
// Returns the predicted rate at the end of the prediction horizon.
func (p *Predictor) PredictMessageRateLinearRegression(window int) float64 {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if len(p.samples) < 2 {
		if len(p.samples) == 1 {
			return p.samples[0].MessageRate
		}
		return 0
	}

	if window <= 0 || window > len(p.samples) {
		window = len(p.samples)
	}

	// Use recent samples for regression
	start := len(p.samples) - window
	n := float64(window)

	// Calculate sums for linear regression: y = slope*t + intercept
	var sumT, sumY, sumTY, sumT2 float64
	for i := 0; i < window; i++ {
		t := float64(i)
		y := p.samples[start+i].MessageRate
		sumT += t
		sumY += y
		sumTY += t * y
		sumT2 += t * t
	}

	// Slope: (n * sumTY - sumT * sumY) / (n * sumT2 - sumT * sumT)
	denominator := n*sumT2 - sumT*sumT
	if math.Abs(denominator) < 1e-10 {
		// No trend, return average
		return sumY / n
	}

	slope := (n*sumTY - sumT*sumY) / denominator
	intercept := (sumY - slope*sumT) / n

	// Predict at the end of the horizon
	lastT := n - 1
	horizonPoints := lastT + float64(p.predictionHorizon.Seconds()) // assume 1 sample per second for simplicity
	predictedRate := slope*horizonPoints + intercept

	// Clamp to non-negative
	if predictedRate < 0 {
		predictedRate = 0
	}

	return predictedRate
}

// GetRecentSamples returns the last N samples (for analysis/debugging).
func (p *Predictor) GetRecentSamples(count int) []LoadSample {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if count <= 0 || count > len(p.samples) {
		count = len(p.samples)
	}

	result := make([]LoadSample, count)
	copy(result, p.samples[len(p.samples)-count:])
	return result
}

// ClearSamples clears all stored samples (for reset/testing).
func (p *Predictor) ClearSamples() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.samples = p.samples[:0]
	p.ewmaRate = 0
}

// GetSampleCount returns the number of samples currently stored.
func (p *Predictor) GetSampleCount() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.samples)
}
