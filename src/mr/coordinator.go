package mr

import (
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type State string

const (
	idle State = "idle"
	run  State = "run"
	done State = "done"
)

type MapTask struct {
	id int

	filename string
	// Assigned Worker Id
	assignid WorkerID
	state    State
}

type ReduceTask struct {
	id          int
	reduceindex int
	assignid    WorkerID
	state       State
}

type Coordinator struct {
	// Your definitions here.
	nReduce int64
	nFile   int64

	mapTasks_mu sync.Mutex
	mapTasks    []MapTask
	mapDone     int64

	// Used to indicated reduce is ready
	// Need to read atmoic
	reduceReady int32

	reduceTask_mu sync.Mutex
	reduceTasks   []ReduceTask
	reduceDone    int64

	// Used to indicated reduce is ready
	// Need to read atmoic
	allDone int32
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// ask for file
func (c *Coordinator) AskForMap(args *AskForMapArgs, reply *AskForMapReply) error {
	if atomic.LoadInt64(&c.mapDone) == c.nFile {
		reply.File = ""
	} else {
		// wait indicates that there is no map task idle but not all map task done
		var wait bool = true
		c.mapTasks_mu.Lock()
		defer c.mapTasks_mu.Unlock()
		for i, task := range c.mapTasks {
			if task.state == idle {
				workid := GetWorkerId()
				reply.TaskId = int64(task.id)
				reply.WorkerId = int64(workid)
				reply.File = task.filename
				reply.Nreduce = c.nReduce

				c.mapTasks[i].state = run
				c.mapTasks[i].assignid = workid
				// TODO : register a timer to monitor the task
				timer := time.NewTimer(10 * time.Second)
				time_i := i
				go func() {
					<-timer.C // wait for timer
					c.mapTasks_mu.Lock()
					defer c.mapTasks_mu.Unlock()
					if c.mapTasks[time_i].state == run {
						c.mapTasks[time_i].state = idle
					}
				}()

				log.Printf("send map task %v to worker %v\n", task.filename, workid)
				wait = false
				break
			}
		}
		reply.Wait = wait
	}

	return nil
}

//
// Called by Client to indicates that the file has been processed
func (c *Coordinator) MapDone(args *MapDoneArgs, reply *MapDoneReply) error {
	c.mapTasks_mu.Lock()
	defer c.mapTasks_mu.Unlock()

	if c.mapTasks[args.TaskId].assignid == WorkerID(args.WorkerID) {
		c.mapTasks[args.TaskId].state = done
		n := atomic.AddInt64(&c.mapDone, 1)
		log.Printf("map task %v done from %v ", c.mapTasks[args.TaskId].filename, args.WorkerID)
		if n == c.nFile {
			// Generates ReduceMap
			c.reduceTask_mu.Lock()
			c.reduceTasks = make([]ReduceTask, c.nReduce)
			defer c.reduceTask_mu.Unlock()
			for i := range c.reduceTasks {
				c.reduceTasks[i].id = i
				c.reduceTasks[i].reduceindex = i
				c.reduceTasks[i].state = idle
			}
			atomic.StoreInt32(&c.reduceReady, 1)
			log.Printf("Generate reduce tasks : %v\n", c.reduceTasks)
		}
	}

	return nil
}

//
// Called by Client to ask for a reduce index
func (c *Coordinator) AskForReduce(args *AskForReduceArgs, reply *AskForReduceReply) error {
	c.reduceTask_mu.Lock()
	defer c.reduceTask_mu.Unlock()

	if atomic.LoadInt32(&c.reduceReady) == 1 {
		reply.Ready = true
		if atomic.LoadInt32(&c.allDone) == 1 {
			reply.Done = true
		} else {
			// wait indicates that there is no reduce task idle but not all reduce task done
			// some are running
			var wait bool = true
			for i, task := range c.reduceTasks {
				if task.state == idle {
					workid := GetWorkerId()
					reply.Done = false
					reply.WorkerID = int64(workid)
					reply.TaskId = int64(task.id)
					reply.Reduceindex = int64(task.reduceindex)

					// NOTE: we can guarantee that mapTask will not be modified in this time
					// TODO : change to write-read lock
					for _, task := range c.mapTasks {
						if task.state != done {
							panic("map task not done")
						}
						reply.InterIDs = append(reply.InterIDs, int64(task.assignid))
					}

					c.reduceTasks[i].assignid = workid
					c.reduceTasks[i].state = run
					log.Printf("send reduce task %v to worker %v\n", task.reduceindex, workid)
					// TODO : register a timer to monitor the task

					timer := time.NewTimer(10 * time.Second)
					time_i := i
					go func() {
						<-timer.C // wait for timer
						c.reduceTask_mu.Lock()
						defer c.reduceTask_mu.Unlock()
						if c.reduceTasks[time_i].state == run {
							c.reduceTasks[time_i].state = idle
						}
					}()
					wait = false
					break
				}
			}
			reply.Wait = wait
		}
	} else {
		reply.Ready = false
	}

	return nil
}

func (c *Coordinator) ReduceDone(args *ReduceDoneArgs, reply *ReduceDoneReply) error {
	c.reduceTask_mu.Lock()
	defer c.reduceTask_mu.Unlock()

	if c.reduceTasks[args.TaskID].assignid == WorkerID(args.WorkerID) {
		c.reduceTasks[args.TaskID].state = done
		log.Printf("reduce task %v done from %v ", c.reduceTasks[args.TaskID].reduceindex, args.WorkerID)
		n := atomic.AddInt64(&c.reduceDone, 1)
		if n == c.nReduce {
			atomic.StoreInt32(&c.allDone, 1)
		}
	}

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {

	// Your code here.
	return atomic.LoadInt64(&c.reduceDone) == c.nReduce
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	log.SetOutput(ioutil.Discard)
	// Generate Map tasks
	tasks := make([]MapTask, len(files))
	for i := 0; i < len(tasks); i++ {
		tasks[i].id = i
		tasks[i].state = idle
		tasks[i].filename = files[i]
	}
	c := Coordinator{
		mapTasks:    tasks,
		nReduce:     int64(nReduce),
		nFile:       int64(len(files)),
		reduceTasks: nil,
		reduceReady: 0,
		allDone:     0,
	}

	log.Printf("%v\n", c.mapTasks)
	log.Printf("nFile: %v\n", c.nFile)
	log.Printf("nReduce: %v\n", nReduce)

	// Your code here.

	c.server()
	return &c
}
