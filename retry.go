package litequeue

import "time"

type Retry struct {
	sleepDuration time.Duration
	RetryFunc     func() error
	numTries      int
}

func NewRetry(numTries int, sleepDuration time.Duration, retryFunc func() error) *Retry {
	return &Retry{
		sleepDuration: sleepDuration,
		RetryFunc:     retryFunc,
		numTries:      numTries,
	}
}

func (r *Retry) Do() error {
	for i := 0; i < r.numTries; i++ {
		err := r.RetryFunc()
		if err == nil {
			break
		}
		time.Sleep(r.sleepDuration)
	}

	return nil
}
