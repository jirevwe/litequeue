package litequeue

import (
	"sync"
	"testing"
	"time"
)
import "github.com/stretchr/testify/require"

type TestTask struct {
	executeFunc    func() error
	wg             *sync.WaitGroup
	mFailure       *sync.Mutex
	failureHandled bool
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

	t.failureHandled = true
}

func (t *TestTask) hitFailureCase() bool {
	t.mFailure.Lock()
	defer t.mFailure.Unlock()

	return t.failureHandled
}

var notifyChan = make(chan *TaskInfo, 1)

func TestWorkerPool_MultipleStartStopDontPanic(t *testing.T) {
	p := NewWorkerPool(5, slogger, notifyChan, notifyChan, NewMux())

	// We're just checking to make sure multiple
	// calls to start or stop don't cause a panic
	p.Start()
	p.Start()

	require.NoError(t, p.Stop())
	require.NoError(t, p.Stop())
}

type counterTest struct {
	count int
	mu    *sync.Mutex
}

func NewCounterTest() *counterTest {
	return &counterTest{
		count: 0,
		mu:    &sync.Mutex{},
	}
}

func (c *counterTest) Inc(t *testing.T) func() error {
	return func() error {
		c.mu.Lock()
		c.count++
		t.Log(c.count)
		c.mu.Unlock()
		return nil
	}
}

func TestWorkerPool_Work(t *testing.T) {
	var tasks []*TestTask
	wg := &sync.WaitGroup{}
	c := NewCounterTest()

	for i := 0; i < 20; i++ {
		wg.Add(1)
		tasks = append(tasks, NewTestTask(c.Inc(t), wg))
	}

	p := NewWorkerPool(5, slogger, notifyChan, notifyChan, NewMux())
	p.Start()

	for _, j := range tasks {
		require.NoError(t, p.AddWork(j))
	}

	// we'll get a timeout failure if the tasks weren't processed
	wg.Wait()

	for taskNum, task := range tasks {
		if task.hitFailureCase() {
			t.Fatalf("error function called on task %d when it shouldn't be", taskNum)
		}
	}
}

func TestWorkerPool_ProcessRemainingTasksAfterStop(t *testing.T) {
	p := NewWorkerPool(4, slogger, notifyChan, notifyChan, NewMux())
	p.Start()
	c := NewCounterTest()

	wg := &sync.WaitGroup{}
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go require.NoError(t, p.AddWork(NewTestTask(c.Inc(t), wg)))
	}

	done := make(chan struct{})
	go func() {
		// wait on our AddWork calls to complete, then signal on the done channel
		wg.Wait()
		done <- struct{}{}
	}()

	// Sleep for a short time to ensure workers have started processing tasks
	time.Sleep(100 * time.Millisecond)

	// Stop the worker pool
	require.NoError(t, p.Stop())

	// wait until either we hit our timeout, or we're told the AddWork calls completed
	select {
	case <-time.After(10 * time.Second):
		t.Fatal("failed because still hanging on AddWork")
	case <-done:
		// This indicates that all tasks have been processed
		return
	}
}

func TestWorkerPool_RaceConditionOnStop(t *testing.T) {
	p := NewWorkerPool(10, slogger, notifyChan, notifyChan, NewMux())
	p.Start()
	c := NewCounterTest()

	wg := &sync.WaitGroup{}
	for i := 0; i < 60; i++ {
		go require.NoError(t, p.AddWork(NewTestTask(c.Inc(t), wg)))
		wg.Add(1)
	}

	done := make(chan struct{})
	go func() {
		// wait on our AddWork calls to complete, then signal on the done channel
		wg.Wait()
		done <- struct{}{}
	}()

	// Sleep for a short time to ensure workers have started processing tasks
	time.Sleep(1 * time.Millisecond)

	// Stop the worker pool concurrently
	go func() {
		require.NoError(t, p.Stop())
	}()

	// wait until either we hit our timeout, or we're told the AddWork calls completed
	select {
	case <-time.After(10 * time.Second):
		t.Fatal("failed because still hanging on AddWork")
	case <-done:
		// this is the success case
		return
	}
}

func TestWorkerPool_ProcessRemainingTasksAfterStop_2(t *testing.T) {
	p := NewWorkerPool(4, slogger, notifyChan, notifyChan, NewMux())
	p.Start()
	c := NewCounterTest()

	wg := &sync.WaitGroup{}
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go require.NoError(t, p.AddWork(NewTask(c.Inc(t), wg)))
	}

	// Sleep for a short time to ensure workers have started processing tasks
	time.Sleep(100 * time.Millisecond)

	// Stop the worker pool
	go require.NoError(t, p.Stop())

	require.Error(t, ErrWorkerPoolClosed, p.AddWork(NewTestTask(c.Inc(t), wg)))

	// Sleep for a short time to ensure workers have started processing tasks
	time.Sleep(1 * time.Second)

	done := make(chan struct{})
	go func() {
		// wait on our AddWork calls to complete, then signal on the done channel
		wg.Wait()
		done <- struct{}{}
	}()

	// wait until either we hit our timeout, or we're told the AddWork calls completed
	select {
	case <-time.After(10 * time.Second):
		t.Fatal("failed because still hanging on AddWork")
	case <-done:
		// This indicates that all tasks have been processed
		return
	}
}
