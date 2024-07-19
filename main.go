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
	lite := NewLiteQueue(ctx, dbPath, slogger)

	err = lite.CreateQueue(ctx, testQueueName)
	if err != nil {
		slogger.Error(err.Error())
	}

	t := NewLiteQueueTask([]byte("hello world!"), slogger)
	err = lite.Write(ctx, testQueueName, t)
	if err != nil {
		slogger.Error(err.Error())
	}

	// start blocks
	lite.Start()
}

func jobAdder(ctx context.Context) {

}
