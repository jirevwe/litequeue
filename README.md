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

