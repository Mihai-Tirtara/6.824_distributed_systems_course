package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

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

type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	WaitTask // no tasks available yet, worker should sleep
	ExitTask // all done, worker should exit
)

type RequestTaskArgs struct {
}

type RequestTaskReply struct {
	TaskType TaskType
	TaskID   int
	Filename string // input file (for map tasks)
	NReduce  int    // so map workers know how many buckets
	NMaps    int    // so reduce workers know how many intermediate files to read
}

type ReportTaskArgs struct {
	TaskType TaskType
	TaskID   int
}

type ReportTaskReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
