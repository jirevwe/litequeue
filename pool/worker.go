package pool

import (
	"fmt"
	"log/slog"
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

	log *slog.Logger

	// channel used to signal up a layer about work status
	notifyChan chan Task
}

func NewWorker(id string, tasks chan Task, quit chan bool, notifyChan chan Task, wg *sync.WaitGroup, log *slog.Logger) *Worker {
	return &Worker{
		id:         id,
		wg:         wg,
		log:        log,
		quit:       quit,
		tasks:      tasks,
		notifyChan: notifyChan,
	}
}

func (w *Worker) Start() {
	w.log.Info(fmt.Sprintf("starting worker %s", w.id))

	defer func() {
		w.wg.Done()
		w.log.Info(fmt.Sprintf("worker %s has been stopped", w.id))
	}()

	for {
		// if there are tasks, process one
		if len(w.tasks) > 0 {
			task, ok := <-w.tasks
			if !ok {
				w.log.Info(fmt.Sprintf("stopping worker %s with closed tasks channel", w.id))
				return
			}

			// notify that the task is "Active"
			//w.notifyChan <- task

			err := task.Execute()
			if err != nil {
				task.OnFailure(err)
			}

			// todo: we write to channel to notify the pool that work was done or failed

			// notify that the task is "Completed" or "Failed"
			//w.notifyChan <- task

			continue
		}

		// check for quit
		select {
		case <-w.quit:
			w.log.Info(fmt.Sprintf("stopping worker %s with quit channel", w.id))

			// when quit is called, how can I ensure that the currently executing task is completed
			// before returning?
			return
		default:
			// do nothing
		}
	}
}
