package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type AskForFileArgs struct {
}

type AskForFileReply struct {
	File    string
	Nreduce int64
}

type MapDoneArgs struct {
	File string
}

type MapDoneReply struct {
}

type AskForReduceArgs struct {
}

//
// Index == -1 means no reduce(already done)
type AskForReduceReply struct {
	Ready bool
	Index int64
	Files []string
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
