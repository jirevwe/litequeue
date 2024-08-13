# Litequeue

# Lite queue is a task queue built on sqlite in Go. The goals of the project are to

- Create queues 
  - with priority
  - without priority
- Queues have handlers (should be assigned at compile time)
- Queues take jobs (which must satisfy a particular interface)
- Can schedule 
  - one-off tasks 
  - periodic tasks
- Jobs can be 
  - paused
  - replayed
  - retried
- 

## Creating Queues
Queues are created at startup and at runtime, the handler for the queue must be passed when the queue is registered. 
```go
err := lite.CreateQueue(ctx, testQueueName, jobAdder(slogger))
if err != nil {
    log.Fatalf(err)
}

func jobAdder(logger *slog.Logger) HandlerFunc {
	return func(ctx context.Context, task *Task) error {
    c := &http.Client{}
    resp, err := c.Get("http://localhost:3000")
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
```

## Adding tasks

```go
id := uuid()
eventByte := event fmt.Sprintf("hello %s!", id) 
delay := time.Second

task := NewTask(id, eventByte, delay)
err = lite.Write(ctx, testQueueName, task)
if err != nil {
    slogger.Error(err.Error())
    return
}
```


