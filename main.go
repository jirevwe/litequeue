package litequeue

import (
	"context"
	"errors"
	"fmt"
	"github.com/jirevwe/litequeue/pool"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
)

func Main() {
	testQueueName := "local_queue"
	slogger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	ctx := context.Background()

	dir, err := os.Getwd()
	if err != nil {
		slogger.Error(err.Error())
	}

	dbPath := filepath.Join(dir, "litequeue.db")
	lite, err := NewLiteQueue(ctx, dbPath, slogger)
	if err != nil {
		slogger.Error(err.Error())
		return
	}

	if lite == nil {
		slogger.Error("lite queue instance is nil")
		return
	}

	err = lite.CreateQueue(ctx, testQueueName, func(task *pool.Task) error {
		// todo: when we run Execute() we need to update the job status in the db
		slogger.Info(fmt.Sprintf("task: %s", string(task.Payload())))
		c := http.Client{}
		resp, err := c.Get("https://httpbin.org/post?one=two")
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
		slogger.Info(string(respStr))

		return nil
	})
	if err != nil {
		slogger.Error(err.Error())
		return
	}

	t := pool.NewTask([]byte("hello world!"), slogger)
	err = lite.Write(ctx, testQueueName, t)
	if err != nil {
		slogger.Error(err.Error())
		return
	}

	// start blocks
	lite.Start()
}

func jobAdder(ctx context.Context) {

}
