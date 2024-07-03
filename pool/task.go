package pool

import (
	"sync"
)

type Task interface {
	// Init sets any required variables needed by the task
	Init(any)

	// Execute performs the work
	Execute() error

	// OnFailure handles any error returned from Execute()
	OnFailure(error)

	// CleanUp cleans up the task
	CleanUp()
}

type TestTask struct {
	executeFunc    func() error
	wg             *sync.WaitGroup
	mFailure       *sync.Mutex
	failureHandled bool
}

func (t *TestTask) Init(a any) {

}

func (t *TestTask) CleanUp() {

}

func NewTestTask(executeFunc func() error, wg *sync.WaitGroup) *TestTask {
	return &TestTask{
		executeFunc: executeFunc,
		wg:          wg,
		mFailure:    &sync.Mutex{},
	}
}

func (t *TestTask) Execute() error {
	if t.wg != nil {
		defer t.wg.Done()
	}

	if t.executeFunc != nil {
		return t.executeFunc()
	}

	return nil
}

func (t *TestTask) OnFailure(e error) {
	t.mFailure.Lock()
	defer t.mFailure.Unlock()

	// todo: use logger to log error

	t.failureHandled = true
}

func (t *TestTask) hitFailureCase() bool {
	t.mFailure.Lock()
	defer t.mFailure.Unlock()

	return t.failureHandled
}
