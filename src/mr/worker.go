package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
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

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	requestArgs := RequestTaskArgs{}
	requestReply := RequestTaskReply{}
	reportArgs := ReportTaskArgs{}
	reportReply := ReportTaskReply{}

	// Your worker implementation here.
	call("Master.RequestTask", &requestArgs, &requestReply)

	for requestReply.TaskType != ExitTask {
		if requestReply.TaskType == MapTask {
			mapTask(mapf, requestReply)
			reportArgs.TaskID = requestReply.TaskID
			reportArgs.TaskType = MapTask
			call("Master.ReportTask", &reportArgs, &reportReply)
		}

		if requestReply.TaskType == ReduceTask {
			reduceTask(reducef, requestReply)
			reportArgs.TaskID = requestReply.TaskID
			reportArgs.TaskType = ReduceTask
			call("Master.ReportTask", &reportArgs, &reportReply)

		}
		if requestReply.TaskType == WaitTask {
			time.Sleep(1 * time.Second)
		}

		requestReply = RequestTaskReply{}
		call("Master.RequestTask", &requestArgs, &requestReply)

	}

	if requestReply.TaskType == ExitTask {
		os.Exit(0)
	}

	return

}

func mapTask(mapf func(string, string) []KeyValue, requestReply RequestTaskReply) {
	//Create temporary files and encoders
	tmpFiles := make([]*os.File, requestReply.NReduce)
	encoders := make([]*json.Encoder, requestReply.NReduce)
	for i := 0; i < requestReply.NReduce; i++ {
		tmpFiles[i], _ = os.CreateTemp("", "mr-tmp")
		encoders[i] = json.NewEncoder(tmpFiles[i])
	}

	//Read the input and create key-value pair map
	file, err := os.Open(requestReply.Filename)
	if err != nil {
		log.Fatalf("cannot open %v", requestReply.Filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", requestReply.Filename)
	}
	err = file.Close()
	if err != nil {
		return
	}
	kva := mapf(requestReply.Filename, string(content))

	//Loop through key-value pairs and encode in tmp files
	for _, kv := range kva {
		bucket := ihash(kv.Key) % requestReply.NReduce
		encoders[bucket].Encode(&kv)
	}

	//Rename all tmp files
	for i := 0; i < requestReply.NReduce; i++ {
		tmpFiles[i].Close()
		os.Rename(tmpFiles[i].Name(), fmt.Sprintf("mr-%d-%d", requestReply.TaskID, i))
	}

}

func reduceTask(reducef func(string, []string) string, requestReply RequestTaskReply) {

	var kva []KeyValue
	oname := "mr-out-" + strconv.Itoa(requestReply.TaskID)
	ofile, _ := os.Create(oname)

	//Read all the intermediate files
	pattern := fmt.Sprintf("mr-*-%d", requestReply.TaskID)
	matches, err := filepath.Glob(pattern)
	if err != nil {
		panic(err)
	}

	for _, filename := range matches {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		//Decode json
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}
	//Sort by key
	sort.Sort(ByKey(kva))

	//Iterate by grouped key

	groupStart := 0

	for groupStart < len(kva) {
		groupEnd := groupStart + 1
		for groupEnd < len(kva) && kva[groupEnd].Key == kva[groupStart].Key {
			groupEnd++
		}
		values := []string{}
		for k := groupStart; k < groupEnd; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[groupStart].Key, values)

		fmt.Fprintf(ofile, "%v %v\n", kva[groupStart].Key, output)

		groupStart = groupEnd
	}

	ofile.Close()
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
