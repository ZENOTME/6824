package mr

import "sync/atomic"

type WorkerID int64

var worker_id WorkerID = 0

func GetWorkerId() WorkerID {
	return WorkerID(atomic.AddInt64((*int64)(&worker_id), 1))
}
