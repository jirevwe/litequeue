package sqlite

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
	"time"
)

func TestSqlite_WriteOne(t *testing.T) {
	queueName := "test_queue"
	s, err := NewSqlite(queueName)
	require.NoError(t, err)

	err = s.Write(context.Background(), queueName, []byte("hello world"))
	require.NoError(t, err)
}

func TestSqlite_WriteConcurrently(t *testing.T) {
	queueName := "test_queue"
	s, err := NewSqlite(queueName)
	require.NoError(t, err)
	wg := &sync.WaitGroup{}
	ctx := context.Background()

	for i := 0; i < 1; i++ {
		go writeOne(t, ctx, s, queueName, wg)
		wg.Add(1)
	}
	wg.Wait()

	require.NoError(t, s.Truncate(ctx, queueName))
}

func TestSqlite_Consume(t *testing.T) {
	queueName := "test_queue"
	s, err := NewSqlite(queueName)
	require.NoError(t, err)
	wg := &sync.WaitGroup{}
	ctx := context.Background()

	for i := 0; i < 2; i++ {
		wg.Add(1)
		writeOne(t, ctx, s, queueName, wg)
	}

	for i := 0; i < 2; i++ {
		wg.Add(1)
		consumeOne(t, ctx, s, queueName, wg)
	}
	wg.Wait()

	require.NoError(t, s.Truncate(ctx, queueName))
}

func writeOne(t *testing.T, ctx context.Context, ss *Sqlite, qName string, w *sync.WaitGroup) {
	err := ss.Write(ctx, qName, []byte(fmt.Sprintf(`{ "now": "%v" }`, time.Now().String())))
	require.NoError(t, err)
	w.Done()
}

func consumeOne(t *testing.T, ctx context.Context, ss *Sqlite, qName string, w *sync.WaitGroup) {
	msg, err := ss.Consume(ctx, qName)
	require.NoError(t, err)
	t.Logf("Got message: %v", string(msg))
	w.Done()
}
