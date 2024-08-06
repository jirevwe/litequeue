package litequeue

import (
	"context"
	"errors"
	"fmt"
	"github.com/oklog/ulid/v2"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

func Main() {
	ctx := context.Background()
	testQueueName := "local_queue"
	slogger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	dir, err := os.Getwd()
	if err != nil {
		slogger.Error(err.Error())
	}

	dbPath := filepath.Join(dir, "litequeue.db")
	lite, err := NewServer(&Config{
		mux:    NewMux(),
		logger: slogger,
		dbPath: dbPath,
	})
	if err != nil {
		slogger.Error(err.Error())
		return
	}

	if lite == nil {
		slogger.Error("lite queue instance is nil")
		return
	}

	err = lite.CreateQueue(ctx, testQueueName, jobAdder(slogger))
	if err != nil {
		slogger.Error(err.Error())
		return
	}

	go lite.Start()

	for {
		t := NewTask([]byte(fmt.Sprintf("hello %s!", ulid.Make().String())), testQueueName)
		err = lite.Write(ctx, testQueueName, t)
		if err != nil {
			slogger.Error(err.Error())
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func jobAdder(logger *slog.Logger) HandlerFunc {
	return func(ctx context.Context, task *Task) error {
		//logger.Info("[inside task]:", "payload", string(task.Payload()))
		c := &http.Client{}
		resp, err := c.Get("https://test-webhook.getconvoy.io/200")
		if err != nil {
			return err
		}

		if resp.StatusCode != http.StatusOK {
			return errors.New(resp.Status)
		}

		// read response body
		respStr, err := io.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		// print response body
		logger.Info("[inside task]:", "resp:", string(respStr))

		return nil
	}
}
