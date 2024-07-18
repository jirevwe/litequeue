package litequeue

import (
	"context"
	"github.com/jirevwe/litequeue/pool"
	"github.com/jirevwe/litequeue/queue/sqlite"
	"log"
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
	s, err := sqlite.NewSqlite(dbPath, slogger)
	if err != nil {
		log.Fatalln(err)
	}

	// todo: LiteQueue should create the queue by calling Sqlite's (or the attached Queue's) APIs
	err = s.CreateQueue(ctx, testQueueName)
	if err != nil {
		slogger.Error(err.Error())
	}

	lite := &LiteQueue{
		workerPool: pool.NewWorkerPool(10, 10),
		ctx:        ctx,
		queue:      s,
		logger:     slogger,
	}

	t := NewLiteQueueTask([]byte("hello world!"), slogger)
	err = lite.Write(ctx, testQueueName, t)
	if err != nil {
		log.Fatalln(err)
	}

	// start blocks
	lite.Start()
}

func jobAdder(ctx context.Context) {

}
