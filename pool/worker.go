package pool

import (
	"log"
	"sync"
)

type Worker struct {
	// the worker id
	id string

	// channel from which the worker consumes work
	tasks chan Task

	// channel to signal the worker to stop working
	quit chan bool

	// used to signal the pool to clean itself up
	wg *sync.WaitGroup
}

func NewWorker(id string, tasks chan Task, quit chan bool, wg *sync.WaitGroup) *Worker {
	return &Worker{
		id:    id,
		wg:    wg,
		quit:  quit,
		tasks: tasks,
	}
}

func (w *Worker) Start() {
	log.Printf("starting worker %s\n", w.id)

	defer func() {
		w.wg.Done()
		log.Printf("worker %s has been stopped\n", w.id)
	}()

	for {
		select {
		case <-w.quit:
			log.Printf("stopping worker %s with quit channel\n", w.id)
			return
		case task, ok := <-w.tasks:
			if !ok {
				log.Printf("stopping worker %s with closed tasks channel\n", w.id)
				return
			}

			if err := task.Execute(); err != nil {
				task.OnFailure(err)
			}
		}
	}
}
