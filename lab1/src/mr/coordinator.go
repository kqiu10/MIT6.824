package mr

import (
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

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

func (c *Coordinator) HandleSuccessJobs() {
	for {
		job, ok := <-c.successJobs
		if !ok {
			break
		}
		switch job.JobType {
		case MapJob:
			log.Println("Start handling Map jobs")
			taskId := strings.Split(job.FileNames[0], "-")[1]
			if _, exist := c.successJobsSet[taskId]; !exist {
				log.Println("Find a new taskIdentifier in success job")
				c.successJobsSet[taskId] = true
				if len(c.successJobsSet) == len(c.rawFiles) {
					c.mutex.Lock()
					defer c.mutex.Unlock()
					if c.addReduce {
						break
					}
					log.Println("Completed reading all map tasks")
					// add Reduce jobs
					for j := 0; j < c.nReduce; j++ {
						var fileNames []string
						for i := 0; i < len(c.rawFiles); i++ {
							taskId := strings.Split(c.rawFiles[i], "-")[1]
							fileNames = append(fileNames, "mr-"+taskId+"-"+strconv.Itoa(j))
						}

						c.availableJobs <- Job{
							JobType:   ReduceJob,
							FileNames: fileNames,
							NReduce:   c.nReduce,
						}

					}
					c.addReduce = true
					log.Printf("after adding reduce, len(c.availableJobs) is %v", len(c.availableJobs))
				}
			}
		case ReduceJob:
			log.Println("Start handling reduce jobs")
			for _, fileName := range job.FileNames {
				taskId := "reduce_" + strings.SplitN(fileName, "-", 2)[1]
				c.successJobsSet[taskId] = true
			}
			if len(c.successJobsSet) == len(c.rawFiles)*(c.nReduce+1) {
				log.Println("All reduce tasks success!")
				close(c.availableJobs)
				close(c.successJobs)
				c.isSuccess = true
			}
		}
	}
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) Mapreduce(args *MapArgs, reply *MapReply) error {
	log.Println("Mapreduce RPC received!")
	for {
		job, ok := <-c.availableJobs
		if !ok {
			log.Println("No more new tasks!")
			*reply = MapReply{Job: Job{
				JobType: NoJob,
				NReduce: c.nReduce,
			}}
			return nil
		}
		reportChannel := make(chan Job)
		id := uuid.New().String()

		c.reportChannelByUUID.Store(id, reportChannel)
		*reply = MapReply{Job: job, TaskId: id}
		go func() {
			log.Println("Wait for reportChannel to send job...")
			select {
			case job := <-reportChannel:
				log.Println("Get job in reportChannel")
				log.Printf("length of file in reportChannel %v", len(job.FileNames))
				c.successJobs <- job

			case <-time.After(10 * time.Second):
				log.Println("timeout in reportChannel")
				c.availableJobs <- job
			}
		}()
		return nil

	}

}

func makeRandString(n int) (string, error) {
	b := make([]byte, n)
	_, err := rand.Read(b)
	if err != nil {
		return "", err
	}
	return base64.URLEncoding.EncodeToString(b)[:n], nil
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
	log.SetOutput(io.Discard)
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
	fmt.Println("Starting map reduce job")
	go c.HandleSuccessJobs()
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
