package ngap

import (
	"fmt"
	"net"
	"runtime"
	"sync"

	"github.com/free5gc/amf/internal/logger"
)

// Task represents a work item to be processed by a worker.
// It contains the UE identifier and the raw NGAP message.
type Task struct {
	UEID    uint64   // AMF-UE-NGAP-ID or RAN-UE-NGAP-ID
	Conn    net.Conn // The network connection for this message
	Message []byte   // The raw NGAP message bytes
}

// Worker represents a goroutine that processes tasks from its dedicated queue.
type Worker struct {
	ID       int
	taskChan chan Task
	stopChan chan struct{} // Signal channel for shutdown
	stopOnce sync.Once     // Ensures stopChan is closed only once
	handler  func(conn net.Conn, msg []byte)
	wg       *sync.WaitGroup
}

// NewWorker creates and starts a new worker goroutine.
func NewWorker(id int, bufferSize int, handler func(conn net.Conn, msg []byte), wg *sync.WaitGroup) *Worker {
	w := &Worker{
		ID:       id,
		taskChan: make(chan Task, bufferSize),
		stopChan: make(chan struct{}),
		handler:  handler,
		wg:       wg,
	}
	wg.Add(1)
	go w.run()
	return w
}

// run is the main event loop for the worker.
func (w *Worker) run() {
	defer func() {
		if p := recover(); p != nil {
			logger.NgapLog.Errorf("Worker %d panic: %v", w.ID, p)
		}
		w.wg.Done()
	}()
	logger.NgapLog.Infof("Worker %d started", w.ID)

	for {
		select {
		case task := <-w.taskChan:
			logger.NgapLog.Debugf("Worker %d processing task for UE ID %d (ensuring per-UE sequentiality)",
				w.ID, task.UEID)
			w.handler(task.Conn, task.Message)

		case <-w.stopChan:
			logger.NgapLog.Infof("Worker %d: shutdown signal received, draining queue...", w.ID)
			w.drainAndExit()
			return
		}
	}
}

// drainAndExit consumes remaining tasks in the buffer without blocking.
func (w *Worker) drainAndExit() {
	for {
		select {
		case task := <-w.taskChan:
			logger.NgapLog.Debugf("Worker %d processing residual task for UE ID %d", w.ID, task.UEID)
			w.handler(task.Conn, task.Message)
		default:
			// Channel is empty, exit safely
			logger.NgapLog.Infof("Worker %d: queue drained, stopped.", w.ID)
			return
		}
	}
}

// Submit submits a task to this worker's queue.
// Returns true if the task was successfully queued, false if the worker is stopped.
func (w *Worker) Submit(task Task) bool {
	select {
	case w.taskChan <- task:
		// Successfully queued (blocks here if buffer is full, providing backpressure)
		return true
	case <-w.stopChan:
		// Worker stopped (either before submission or while waiting). Unblock and return false.
		logger.NgapLog.Warnf("Worker %d stopped, rejecting task for UE ID %d", w.ID, task.UEID)
		return false
	}
}

// Stop signals the worker to shut down.
func (w *Worker) Stop() {
	w.stopOnce.Do(func() {
		close(w.stopChan)
	})
}

// UEScheduler distributes NGAP tasks to workers based on UE ID.
type UEScheduler struct {
	workers        []*Worker
	numWorkers     int
	taskBufferSize int
	handler        func(conn net.Conn, msg []byte)
	workerLock     sync.RWMutex
	wg             sync.WaitGroup
}

// NewUEScheduler creates a new UE scheduler with the specified number of workers.
func NewUEScheduler(numWorkers int, taskBufferSize int, handler func(conn net.Conn, msg []byte)) *UEScheduler {
	if numWorkers <= 0 {
		numWorkers = runtime.NumCPU()
	}

	logger.NgapLog.Infof("Initializing UE Scheduler with %d workers", numWorkers)

	scheduler := &UEScheduler{
		workers:        make([]*Worker, numWorkers),
		numWorkers:     numWorkers,
		taskBufferSize: taskBufferSize,
		handler:        handler,
	}

	for i := 0; i < numWorkers; i++ {
		scheduler.workers[i] = NewWorker(i, taskBufferSize, handler, &scheduler.wg)
	}

	return scheduler
}

// DispatchTask dispatches a task to the appropriate worker based on UE ID hashing.
func (s *UEScheduler) DispatchTask(task Task) bool {
	s.workerLock.RLock()
	defer s.workerLock.RUnlock()

	workerIndex := s.hashUEID(task.UEID)
	if workerIndex < 0 || workerIndex >= len(s.workers) {
		logger.NgapLog.Errorf("Invalid worker index %d for UE ID %d", workerIndex, task.UEID)
		return false
	}
	worker := s.workers[workerIndex]

	logger.NgapLog.Debugf("Dispatching UE ID %d to Worker %d (hash-based routing)",
		task.UEID, workerIndex)
	return worker.Submit(task)
}

// hashUEID computes a hash of the UE ID and maps it to a worker index.
// This ensures all messages for the same UE go to the same worker.
func (s *UEScheduler) hashUEID(ueID uint64) int {
	return int(ueID % uint64(s.numWorkers))
}

// Shutdown gracefully shuts down all workers.
func (s *UEScheduler) Shutdown() {
	logger.NgapLog.Info("Shutting down UE Scheduler and all workers...")

	for i, worker := range s.workers {
		logger.NgapLog.Infof("Closing task channel for Worker %d", i)
		worker.Stop()
	}

	s.wg.Wait()
	logger.NgapLog.Info("All workers shut down successfully")
}

// Global scheduler instance
var (
	globalScheduler     *UEScheduler
	globalSchedulerOnce sync.Once
	schedulerMutex      sync.RWMutex
)

// InitScheduler initializes the global UE scheduler.
// Should be called once during AMF startup.
func InitScheduler(numWorkers int, taskBufferSize int, handler func(conn net.Conn, msg []byte)) {
	globalSchedulerOnce.Do(func() {
		// Apply sensible defaults if invalid values provided
		if numWorkers <= 0 {
			numWorkers = runtime.NumCPU()
		}
		if taskBufferSize <= 0 {
			taskBufferSize = 4096 // Default buffer size
		}

		schedulerMutex.Lock()
		defer schedulerMutex.Unlock()

		globalScheduler = NewUEScheduler(numWorkers, taskBufferSize, handler)
		logger.NgapLog.Infof("Global UE Scheduler initialized with %d workers, buffer size %d",
			numWorkers, taskBufferSize)
	})
}

// GetScheduler returns the global scheduler instance.
func GetScheduler() (*UEScheduler, error) {
	schedulerMutex.RLock()
	defer schedulerMutex.RUnlock()

	if globalScheduler == nil {
		return nil, fmt.Errorf("scheduler not initialized")
	}
	return globalScheduler, nil
}

// ShutdownScheduler gracefully shuts down the global scheduler.
func ShutdownScheduler() {
	schedulerMutex.Lock()
	defer schedulerMutex.Unlock()

	if globalScheduler != nil {
		globalScheduler.Shutdown()
	}
}

// GetTotalQueueDepth returns the sum of queued tasks across all workers.
func GetTotalQueueDepth() int {
	schedulerMutex.RLock()
	defer schedulerMutex.RUnlock()

	if globalScheduler == nil {
		return 0
	}

	total := 0
	for _, worker := range globalScheduler.workers {
		total += len(worker.taskChan)
	}
	return total
}

// GetWorkerCount returns the current number of active workers.
func GetWorkerCount() int {
	schedulerMutex.RLock()
	defer schedulerMutex.RUnlock()

	if globalScheduler == nil {
		return 0
	}
	return globalScheduler.numWorkers
}

// GetWorkerQueueDepths returns queue depth for each worker.
func GetWorkerQueueDepths() map[int]int {
	schedulerMutex.RLock()
	defer schedulerMutex.RUnlock()

	depths := make(map[int]int)
	if globalScheduler == nil {
		return depths
	}

	for _, worker := range globalScheduler.workers {
		depths[worker.ID] = len(worker.taskChan)
	}
	return depths
}

// GetAverageQueueDepth returns the average queue depth across workers.
func GetAverageQueueDepth() float64 {
	schedulerMutex.RLock()
	defer schedulerMutex.RUnlock()

	if globalScheduler == nil || globalScheduler.numWorkers == 0 {
		return 0
	}

	total := 0
	for _, worker := range globalScheduler.workers {
		total += len(worker.taskChan)
	}
	return float64(total) / float64(globalScheduler.numWorkers)
}

// GetMaxQueueDepth returns the maximum queue depth among all workers.
func GetMaxQueueDepth() int {
	schedulerMutex.RLock()
	defer schedulerMutex.RUnlock()

	if globalScheduler == nil {
		return 0
	}

	max := 0
	for _, worker := range globalScheduler.workers {
		depth := len(worker.taskChan)
		if depth > max {
			max = depth
		}
	}
	return max
}

// GetTaskBufferSize returns the current buffer size in use by the scheduler.
func GetTaskBufferSize() int {
	schedulerMutex.RLock()
	defer schedulerMutex.RUnlock()

	if globalScheduler == nil {
		return 0
	}
	return globalScheduler.taskBufferSize
}

// ScaleWorkers adjusts the number of workers and buffer size for the scheduler.
func ScaleWorkers(targetWorkers int, targetBufferSize int) error {
	schedulerMutex.Lock()
	defer schedulerMutex.Unlock()

	if globalScheduler == nil {
		return fmt.Errorf("scheduler not initialized")
	}
	if targetWorkers <= 0 {
		return fmt.Errorf("targetWorkers must be > 0")
	}
	if targetBufferSize <= 0 {
		targetBufferSize = globalScheduler.taskBufferSize
	}

	return globalScheduler.scaleWorkers(targetWorkers, targetBufferSize)
}

func (s *UEScheduler) scaleWorkers(targetWorkers int, targetBufferSize int) error {
	s.workerLock.Lock()
	defer s.workerLock.Unlock()

	currentWorkers := s.numWorkers
	if targetWorkers == currentWorkers {
		if targetBufferSize != s.taskBufferSize {
			logger.NgapLog.Infof("Updating future worker buffer size from %d to %d", s.taskBufferSize, targetBufferSize)
			s.taskBufferSize = targetBufferSize
		}
		return nil
	}

	if targetWorkers > currentWorkers {
		logger.NgapLog.Infof("Scaling NGAP workers up from %d to %d", currentWorkers, targetWorkers)
		for i := currentWorkers; i < targetWorkers; i++ {
			worker := NewWorker(i, targetBufferSize, s.handler, &s.wg)
			s.workers = append(s.workers, worker)
		}
		s.numWorkers = targetWorkers
		s.taskBufferSize = targetBufferSize
		return nil
	}

	// Scale down: stop excess workers and remove them from the worker slice.
	logger.NgapLog.Infof("Scaling NGAP workers down from %d to %d", currentWorkers, targetWorkers)
	for i := currentWorkers - 1; i >= targetWorkers; i-- {
		worker := s.workers[i]
		worker.Stop()
		s.workers = s.workers[:i]
	}
	s.numWorkers = targetWorkers
	if targetBufferSize != s.taskBufferSize {
		logger.NgapLog.Infof("Updating future worker buffer size from %d to %d", s.taskBufferSize, targetBufferSize)
		s.taskBufferSize = targetBufferSize
	}
	return nil
}

