package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	reguestArgs := RequestTaskArgs{}
	requestRepy := RequestTaskReply{}
	reportArgs := ReportTaskArgs{}
	reportRepy := ReportTaskReply{}

	// Your worker implementation here.
	call("Master.RequestTask", &reguestArgs, &requestRepy)

	if requestRepy.TaskType == MapTask {

		//Create temporary files and encoders
		tmpFiles := make([]*os.File, requestRepy.NReduce)
		encoders := make([]*json.Encoder, requestRepy.NReduce)
		for i := 0; i < requestRepy.NReduce; i++ {
			tmpFiles[i], _ = os.CreateTemp("", "mr-tmp")
			encoders[i] = json.NewEncoder(tmpFiles[i])
		}

		//Read the input and create key-value pair map
		file, err := os.Open(requestRepy.Filename)
		if err != nil {
			log.Fatalf("cannot open %v", requestRepy.Filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", requestRepy.Filename)
		}
		err = file.Close()
		if err != nil {
			return
		}
		kva := mapf(requestRepy.Filename, string(content))

		//Loop through key-value pairs and encode in tmp files
		for _, kv := range kva {
			bucket := ihash(kv.Key) % requestRepy.NReduce
			encoders[bucket].Encode(&kv)
		}

		//Rename all tmp files
		for i := 0; i < requestRepy.NReduce; i++ {
			tmpFiles[i].Close()
			os.Rename(tmpFiles[i].Name(), fmt.Sprintf("mr-%d-%d", requestRepy.TaskID, i))
		}

		reportArgs.TaskID = requestRepy.TaskID
		reportArgs.TaskType = MapTask

		call("Master.ReportTask", &reportArgs, &reportRepy)
	}

	if requestRepy.TaskType == ReduceTask {

	}

	if requestRepy.TaskType == WaitTask {
		time.Sleep(10 * time.Second)
	}

	if requestRepy.TaskType == ExitTask {

	}

	//Our workers needs to request a task from master

	//Depending on the task type:
	//If task is Map: we call the map function write to a temporary file once it's done rename the file
	// If task is wait: time.sleep(10)
	//If task is reduce call reduce function on it

	// uncomment to send the Example RPC to the master.
	// CallExample()

}

func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
