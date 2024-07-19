package litequeue

import (
	"context"
	"log/slog"
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

	err = lite.CreateQueue(ctx, testQueueName)
	if err != nil {
		slogger.Error(err.Error())
		return
	}

	t := NewLiteQueueTask([]byte("hello world!"), slogger)
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
