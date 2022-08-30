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
)

type Coordinator struct {
	// Your definitions here.
	nFile int

	mu           sync.Mutex
	remain_files []string

	reduce_mu    sync.Mutex
	reduce_files []string

	nReduce int64

	ReduceIndex int64
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
func (c *Coordinator) AskForFile(args *AskForFileArgs, reply *AskForFileReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.remain_files) == 0 {
		reply.File = ""
	} else {
		log.Println("send file:", c.remain_files[0])
		reply.File = c.remain_files[0]
		c.remain_files = c.remain_files[1:]
	}
	reply.Nreduce = c.nReduce
	return nil
}

//
// Called by Client to indicates that the file has been processed
func (c *Coordinator) MapDone(args *MapDoneArgs, reply *MapDoneReply) error {
	c.reduce_mu.Lock()
	defer c.reduce_mu.Unlock()
	c.reduce_files = append(c.reduce_files, args.File)
	log.Printf("receive map done: %v\n", args.File)
	return nil
}

//
// Called by Client to ask for a reduce index
func (c *Coordinator) AskForReduce(args *AskForReduceArgs, reply *AskForReduceReply) error {
	c.reduce_mu.Lock()
	defer c.reduce_mu.Unlock()
	if len(c.reduce_files) < c.nFile {
		reply.Ready = false
		reply.Index = -1
	} else if atomic.LoadInt64(&c.ReduceIndex) == c.nReduce {
		reply.Ready = true
		reply.Index = -1
	} else {
		reply.Ready = true
		reply.Index = atomic.LoadInt64(&c.ReduceIndex)
		reply.Files = c.reduce_files
		log.Printf("send reduce job, index : %v\n", reply.Index)
		atomic.AddInt64(&c.ReduceIndex, 1)
	}
	return nil
}

func LoadInt32(i int) {
	panic("unimplemented")
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
	return atomic.LoadInt64(&c.ReduceIndex) == c.nReduce
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	log.SetOutput(ioutil.Discard)
	c := Coordinator{
		remain_files: files,
		nReduce:      int64(nReduce),
		nFile:        len(files),
	}
	log.Printf("%v\n", c.remain_files)
	log.Printf("nFile: %v\n", c.nFile)
	log.Printf("nReduce: %v\n", nReduce)

	// Your code here.

	c.server()
	return &c
}
