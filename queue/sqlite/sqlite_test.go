package sqlite

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/require"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

var slogger = slog.New(slog.NewTextHandler(os.Stdout, nil))

func TestSqlite_WriteOne(t *testing.T) {
	ctx := context.Background()
	queueName := "test"

	dir, err := os.Getwd()
	if err != nil {
		slogger.Error(err.Error())
	}

	dbPath := filepath.Join(dir, "../../litequeue.db")

	s, err := NewSqlite(dbPath, slogger)
	require.NoError(t, err)

	err = s.CreateQueue(ctx, queueName)
	require.NoError(t, err)

	err = s.Write(ctx, queueName, []byte("hello world"))
	require.NoError(t, err)

	require.NoError(t, s.Truncate(ctx, queueName))
}

func TestSqlite_WriteConcurrently(t *testing.T) {
	ctx := context.Background()
	queueName := "test"
	wg := &sync.WaitGroup{}

	dir, err := os.Getwd()
	if err != nil {
		slogger.Error(err.Error())
	}

	dbPath := filepath.Join(dir, "../../litequeue.db")

	s, err := NewSqlite(dbPath, slogger)
	require.NoError(t, err)

	err = s.CreateQueue(ctx, queueName)
	require.NoError(t, err)

	message := []byte("hello world")

	for i := 0; i < 10; i++ {
		go writeOne(t, ctx, s, queueName, message, wg)
		wg.Add(1)
	}
	wg.Wait()

	require.NoError(t, s.Truncate(ctx, queueName))
}

func TestSqlite_Consume(t *testing.T) {
	ctx := context.Background()
	queueName := "test"

	dir, err := os.Getwd()
	if err != nil {
		slogger.Error(err.Error())
	}

	dbPath := filepath.Join(dir, "../../litequeue.db")

	s, err := NewSqlite(dbPath, slogger)
	require.NoError(t, err)

	err = s.CreateQueue(ctx, queueName)
	require.NoError(t, err)

	message := []byte("hello world")

	for i := 0; i < 2; i++ {
		writeOne(t, ctx, s, queueName, []byte(fmt.Sprintf("%s_%d", message, i)), nil)
	}

	time.Sleep(2 * time.Second)

	for i := 0; i < 2; i++ {
		msg, consumeErr := s.Consume(ctx, queueName)
		require.NoError(t, consumeErr)
		require.Equal(t, fmt.Sprintf("%s_%d", message, i), msg.Message)
	}

	require.NoError(t, s.Truncate(ctx, queueName))
}

func TestSqlite_Truncate(t *testing.T) {
	ctx := context.Background()
	queueName := "test"

	dir, err := os.Getwd()
	if err != nil {
		slogger.Error(err.Error())
	}

	dbPath := filepath.Join(dir, "../../litequeue.db")

	s, err := NewSqlite(dbPath, slogger)
	require.NoError(t, err)

	err = s.CreateQueue(ctx, queueName)
	require.NoError(t, err)

	message := []byte("hello world")

	for i := 0; i < 2; i++ {
		writeOne(t, ctx, s, queueName, []byte(fmt.Sprintf("%s_%d", message, i)), nil)
	}

	time.Sleep(2 * time.Second)

	msgIds := make([]string, 2)
	for i := 0; i < 2; i++ {
		msg, consumeErr := s.Consume(ctx, queueName)
		require.NoError(t, consumeErr)
		require.Equal(t, fmt.Sprintf("%s_%d", message, i), msg.Message)
		msgIds[i] = msg.Id
	}

	for i := 0; i < 2; i++ {
		deleteErr := s.Delete(ctx, queueName, msgIds[i])
		require.NoError(t, deleteErr)
	}
}

func writeOne(t *testing.T, ctx context.Context, ss *Sqlite, qName string, message []byte, w *sync.WaitGroup) {
	err := ss.Write(ctx, qName, message)
	require.NoError(t, err)

	if w != nil {
		w.Done()
	}
}
