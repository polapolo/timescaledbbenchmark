package pkg

import "log"

// WorkerPool is a contract for Worker Pool implementation
type WorkerPool interface {
	Run()
	AddTask(task func())
}

type workerPool struct {
	maxWorker   int
	queuedTaskC chan func()
}

// NewWorkerPool will create an instance of WorkerPool.
func NewWorkerPool(maxWorker int) WorkerPool {
	wp := &workerPool{
		maxWorker:   maxWorker,
		queuedTaskC: make(chan func()),
	}

	return wp
}

func (wp *workerPool) Run() {
	for i := 1; i <= wp.maxWorker; i++ {
		log.Printf("[WorkerPool] Worker %d has been spawned", i)
		go func(workerID int) {
			for task := range wp.queuedTaskC {
				log.Printf("[WorkerPool] Worker %d start processing task", workerID)
				task()
				log.Printf("[WorkerPool] Worker %d finish processing task", workerID)
			}
		}(i)
	}
}

func (wp *workerPool) AddTask(task func()) {
	wp.queuedTaskC <- task
}

func (wp *workerPool) GetTotalQueuedTask() int {
	return len(wp.queuedTaskC)
}
