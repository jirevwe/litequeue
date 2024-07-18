package litequeue

import (
	"errors"
	"fmt"
	"github.com/jirevwe/litequeue/packer"
	"io"
	"log/slog"
	"net/http"
)

type Task struct {
	Message []byte
	log     *slog.Logger
}

func NewLiteQueueTask(message []byte, log *slog.Logger) *Task {
	return &Task{
		Message: message,
		log:     log,
	}
}

func (l *Task) Execute() error {
	// todo: when we run Execute() we need to update the job status in the db
	l.log.Info(fmt.Sprintf("task: %s", string(l.Message)))
	c := http.Client{}
	resp, err := c.Get("https://httpbin.org/get?one=two")
	if err != nil {
		return err
	}

	if resp.StatusCode != 200 {
		return errors.New(resp.Status)
	}

	// read response body
	respStr, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// print response body
	l.log.Info(string(respStr))

	return nil
}

func (l *Task) OnFailure(err error) {
	l.log.Error(fmt.Sprintf("error: %s", err))
}

func (l *Task) Marshal() ([]byte, error) {
	return packer.EncodeMessage(l.Message)
}
