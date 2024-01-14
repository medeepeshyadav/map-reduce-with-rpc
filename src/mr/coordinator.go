package mr

import (
	"encoding/gob"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// Constants for task types
const (
	IDLE        = 0
	IN_PROGRESS = 1
	COMPLETED   = 2
)

type Coordinator struct {
	MapTasks             []*MapTask
	ReduceTasks          []*ReduceTask
	RemainingMapTasks    int
	RemainingReduceTasks int
	mutex                sync.Mutex
}

type Task struct {
	WorkerID  string
	Status    int
	StartedAt time.Time
}

func (task *Task) Assign(WorkerID string) {
	task.Status = IN_PROGRESS
	task.WorkerID = WorkerID
	task.StartedAt = time.Now()
}

type MapTask struct {
	InputFile string
	NReduce   int
	Task
}

type ReduceTask struct {
	Region    int
	Locations []string
	Task
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

// Your code here -- RPC handlers for the worker to call.
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	gob.Register(MapTask{})
	gob.Register(ReduceTask{})

	// check for unstarted map tasks
	fmt.Println("No. of map tasks remaining:", c.RemainingMapTasks)
	fmt.Println("No. of reduce tasks remaining:", c.RemainingReduceTasks)
	if c.RemainingMapTasks != 0 {
		for _, mapTask := range c.MapTasks {
			if mapTask.Status == IDLE {
				mapTask.Status = IN_PROGRESS
				fmt.Printf("Coordinator: assigning map task: %v\n", mapTask.InputFile)
				mapTask.Assign(args.WorkerID)
				*reply = mapTask
				c.RemainingMapTasks -= 1
				break
			}
		}
		return nil
	}

	// check for unstarted reduce tasks
	if c.RemainingReduceTasks != 0 {
		for _, reduceTask := range c.ReduceTasks {
			if reduceTask.Status == IDLE {
				reduceTask.Status = IN_PROGRESS
				fmt.Printf("Coordinator: assigning reduce task: %v\n", reduceTask.Region)
				reduceTask.Assign(args.WorkerID)
				*reply = reduceTask
				c.RemainingReduceTasks -= 1
				break
			}
		}
		return nil
	}

	// no more tasks
	fmt.Println("Coordinator: No idle task found")
	reply = nil
	return nil
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false
	if c.RemainingMapTasks == 0 && c.RemainingReduceTasks == 0 {
		ret = true
	}
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		MapTasks:             make([]*MapTask, len(files)),
		ReduceTasks:          make([]*ReduceTask, nReduce),
		RemainingMapTasks:    len(files),
		RemainingReduceTasks: nReduce,
		mutex:                sync.Mutex{},
	}

	// Your code here.
	// Initialize map task
	for i, file := range files {
		c.MapTasks[i] = &MapTask{
			InputFile: file,
			NReduce:   nReduce,
			Task: Task{
				Status: IDLE,
			},
		}
	}

	// Initialize reduce task
	for i := 0; i < nReduce; i++ {
		c.ReduceTasks[i] = &ReduceTask{
			Region: nReduce + 1,
			Task: Task{
				Status: IDLE,
			},
		}
	}

	fmt.Printf("Coordinator initialized with %v Map Tasks\n", len(files))
	fmt.Printf("Coordinator initialized with %v Reduce Tasks\n", nReduce)

	c.server()
	return &c
}
