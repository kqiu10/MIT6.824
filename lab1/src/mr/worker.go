package mr

import (
	"bufio"
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
	//log.SetOutput(io.Discard)
	for {
		args := &MapArgs{}
		reply := &MapReply{}
		log.Println("Getting Task...")
		call("Coordinator.Mapreduce", args, reply)
		job := reply.Job
		log.Println("details of Task.job", job)
		if job.JobType == NoJob {
			break
		}
		switch job.JobType {
		case MapJob:
			log.Printf("Working on the map work, %v, File name, %v\n", reply.TaskId, job.FileNames[0])
			doMap(job, reply.TaskId, mapf)
		case ReduceJob:
			log.Printf("Working on the reduce work, %v, File name, %v\n", reply.TaskId, job.FileNames[0])
			doReduce(reply.Job, reply.TaskId, reducef)
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
	file.Close()

	entry := mapf(job.FileNames[0], string(content))
	sort.Sort(ByKey(entry))
	tmpFiles := make([]*os.File, job.NReduce)
	for i := 0; i < job.NReduce; i++ {
		tmpFiles[i], err = os.CreateTemp("./", "temp_map_")
		if err != nil {
			log.Fatal("error occur creating temp file")
		}
	}
	defer func() {
		for i := 0; i < job.NReduce; i++ {
			tmpFiles[i].Close()
		}
	}()

	for _, kv := range entry {
		hash := ihash(kv.Key) % job.NReduce
		fmt.Fprintf(tmpFiles[hash], "%v, %v\n", kv.Key, kv.Value)
	}
	for i := 0; i < job.NReduce; i++ {
		taskId := strings.Split(job.FileNames[0], "-")[1]
		os.Rename(tmpFiles[i].Name(), "mr-"+taskId+"-"+strconv.Itoa(i))
	}

	newArgs := &ReportSuccessArgs{job, taskId}
	newReply := &ReportSuccessReply{}
	log.Println("Job finishes, calling Coordinator.ReportSuccess", newArgs)
	log.Printf("Map Job %v completed, taskId: %v, sending report", job.JobType, taskId)

	call("Coordinator.ReportSuccess", newArgs, newReply)

}

func doReduce(job Job, taskId string, reducef func(string, []string) string) {
	entry := make([][]KeyValue, len(job.FileNames))
	for key, fileName := range job.FileNames {
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("cannot open %v", fileName)
		}
		defer file.Close()
		scanner := bufio.NewScanner(file)

		for scanner.Scan() {
			tokens := strings.Split(scanner.Text(), " ")
			entry[key] = append(entry[key], KeyValue{tokens[0], tokens[1]})
		}

	}
	ids := make([]int, len(job.FileNames))
	ofile, _ := os.CreateTemp("./", "temp_reduce_")
	defer ofile.Close()
	values := []string{}
	prevKey := ""
	/**
	For each key-value pair in entry
	count the number of keys and store them in ids
	store all values under the same key in values
	*/
	for {
		findNext := false
		var nextI int
		for i, pair := range entry {
			if ids[i] < len(pair) {
				if !findNext {
					findNext = true
					nextI = i
				} else if strings.Compare(pair[ids[i]].Key, entry[nextI][ids[nextI]].Key) < 0 {
					nextI = i
				}
			}
		}
		if findNext {
			nextKV := entry[nextI][ids[nextI]]
			if prevKey == "" {
				prevKey = nextKV.Key
				values = append(values, nextKV.Value)
			} else {
				if nextKV.Key == prevKey {
					values = append(values, nextKV.Value)
				} else {
					fmt.Fprintf(ofile, "%v %v\n", prevKey, reducef(prevKey, values))
					prevKey = nextKV.Key
					values = []string{nextKV.Value}
				}
			}
			ids[nextI]++
		} else {
			break
		}
	}

	if prevKey != "" {
		fmt.Fprintf(ofile, "%v, %v\n", prevKey, reducef(prevKey, values))
	}
	taskIdentifier := strings.Split(job.FileNames[0], "-")[2]
	os.Rename(ofile.Name(), "mr-out-"+taskIdentifier)

	newArgs := &ReportSuccessArgs{job, taskId}
	newReply := &ReportSuccessReply{}

	log.Printf("Reduce Job %v completed, sending report", job.FileNames)

	call("Coordinator.ReportSuccess", newArgs, newReply)

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
