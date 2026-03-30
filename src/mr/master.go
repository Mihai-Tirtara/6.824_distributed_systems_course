package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Master struct {
	mapTasks    []Task
	reduceTasks []Task
	nReduce     int
	nMaps       int
	lock        sync.Mutex
}

type Task struct {
	ID        int
	filename  string
	State     TaskState
	StartTime time.Time
}
type TaskState int

const (
	Idle TaskState = iota
	InProgress
	Done
)

// Your code here -- RPC handlers for the worker to call.

func (m *Master) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	checkTasks(m.mapTasks)

	for i := range m.mapTasks {
		if m.mapTasks[i].State == Idle {
			m.mapTasks[i].State = InProgress
			m.mapTasks[i].StartTime = time.Now()

			reply.TaskID = m.mapTasks[i].ID
			reply.TaskType = MapTask
			reply.Filename = m.mapTasks[i].filename
			reply.NReduce = m.nReduce
			return nil
		}
	}

	if !allTasksDone(m.mapTasks) {
		reply.TaskType = WaitTask
		return nil
	}

	checkTasks(m.reduceTasks)

	for i := range m.reduceTasks {
		if m.reduceTasks[i].State == Idle {
			m.reduceTasks[i].State = InProgress
			m.reduceTasks[i].StartTime = time.Now()

			reply.TaskID = m.reduceTasks[i].ID
			reply.TaskType = ReduceTask
			reply.NMaps = m.nMaps
			return nil
		}
	}

	if !allTasksDone(m.reduceTasks) {
		reply.TaskType = WaitTask
		return nil
	}

	if allTasksDone(m.reduceTasks) {
		reply.TaskType = ExitTask
		return nil
	}

	return nil
}

func (m *Master) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	if args.TaskType == MapTask || args.TaskType == ReduceTask {
		m.mapTasks[args.TaskID].State = Done
		return nil
	}
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	ret := false

	if allTasksDone(m.mapTasks) && allTasksDone(m.reduceTasks) {
		ret = true
	}

	return ret
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.nReduce = nReduce
	m.nMaps = len(files)
	m.mapTasks = make([]Task, len(files))
	m.reduceTasks = make([]Task, nReduce)

	for i := 0; i < len(files); i++ {
		m.mapTasks[i].ID = i
		m.mapTasks[i].State = Idle
		m.mapTasks[i].StartTime = time.Time{}
		m.mapTasks[i].filename = files[i]
	}

	for i := 0; i < nReduce; i++ {
		m.reduceTasks[i].ID = i
		m.reduceTasks[i].State = Idle
		m.reduceTasks[i].StartTime = time.Time{}
	}

	m.server()
	return &m
}

func checkTasks(tasks []Task) {
	for i := range tasks {
		if tasks[i].State == InProgress && time.Now().Sub(tasks[i].StartTime).Seconds() > 10 {
			tasks[i].State = Idle
			tasks[i].StartTime = time.Time{}
		}
	}
}

func allTasksDone(tasks []Task) bool {
	for i := range tasks {
		if tasks[i].State != Done {
			return false
		}

	}
	return true
}
