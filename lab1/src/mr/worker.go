package mr

import (
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// Leverage code in mrsequential.go file
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.

	for {
		args := &MapArgs{}
		reply := &MapReply{}
		log.Println("Getting Task...")
		call("Coordinator.GetTask", args, reply)
		job := reply.Job
		if job.JobType == NoJob {
			break
		}
		switch job.JobType {
		case MapJob:
			log.Printf("Working on the map work, %v, File name, %v\n", reply.TaskId, job.FileNames[0])
			doMap(job, reply.TaskId, mapf)
		case ReduceJob:
			log.Printf("Working on the reduce work, %v, File name, %v\n", reply.TaskId, job.FileNames[0])
			doReduce(job, reply.TaskId, reducef)
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	//CallExample()

}

func doMap(job Job, taskId string, mapf func(string, string) []KeyValue) {
	file, err := os.Open(job.FileNames[0])
	if err != nil {
		log.Fatalf("Can't openfile, %v", job.FileNames)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read file, %v", job.FileNames)
	}
	err = file.Close()
	if err != nil {
		log.Fatalf("Cannot close file, %v", job.FileNames)
	}
	kva := mapf(job.FileNames[0], string(content))
	sort.Sort(ByKey(kva))
	tmps := make([]*os.File, job.NReduce)
	for i := 0; i < job.NReduce; i++ {
		tmps[i], err = os.CreateTemp("./", "temp_map_")
		if err != nil {
			log.Fatal("error occur creating temp file")
		}
	}
	defer func() {
		for i := 0; i < job.NReduce; i++ {
			err := tmps[i].Close()
			if err != nil {
				log.Fatal("error occur closing temp file")
			}
		}
	}()

	for _, kv := range kva {
		hash := ihash(kv.Key) % job.NReduce
		fmt.Fprintf(tmps[hash], "%v, %v\n", kv.Key, kv.Value)
	}
	for i := 0; i < job.NReduce; i++ {
		taskId := strings.Split(job.FileNames[0], "-")[1]
		os.Rename(tmps[i].Name(), "mr-"+taskId+"-"+strconv.Itoa(i))
	}

	newArgs := &ReportSuccessArgs{job, taskId}
	newReply := &ReportSuccessReply{}
	log.Printf("Job %v completed, sending report", job.FileNames)

	call("Coordinator.ReportSuccess", newArgs, newReply)

}

func doReduce(job Job, taskId string, reducef func(string, string) string) {

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
