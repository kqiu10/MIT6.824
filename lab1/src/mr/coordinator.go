package mr

import (
	"errors"
	"log"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	jobs                []Job
	rawFiles            []string
	reportChannelByUUID sync.Map
	availableJobs       chan Job
	successJobs         chan Job
	nReduce             int
	successJobsSet      map[string]bool
	isSuccess           bool
	mutex               sync.Mutex
	addReduce           bool
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) Mapreduce(args *MapArgs, reply *MapReply) error {

}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	return c.isSuccess
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		rawFiles:       files,
		availableJobs:  make(chan Job, 100),
		successJobs:    make(chan Job, 100),
		nReduce:        nReduce,
		isSuccess:      false,
		successJobsSet: make(map[string]bool),
		addReduce:      false,
	}

	// Your code here.
	for _, fileName := range files {
		c.availableJobs <- Job{
			JobType:   MapJob,
			FileNames: []string{fileName},
			NReduce:   nReduce,
		}
	}
	go c.handleSuccessJobs()
	c.server()
	return &c
}

func (c *Coordinator) ReportSuccess(args *ReportSuccessArgs, reply *ReportSuccessReply) error {
	log.Printf("ReportSuccess job file length: %v", len(args.Job.FileNames))
	value, ok := c.reportChannelByUUID.Load(args.TaskId)
	if !ok {
		return errors.New("cannot read given uuid")
	}
	reportChannel := value.(chan Job)
	reportChannel <- args.Job
	return nil
}
