package mr

import (
	"encoding/gob"
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
	Locations            []string
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

func (c *Coordinator) doesContain(workerId string) bool {
	for _, loc := range c.Locations {
		if loc == workerId {
			return true
		}
	}
	return false
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
	if c.RemainingMapTasks != 0 {
		if !c.doesContain(args.WorkerID) {
			c.Locations = append(c.Locations, args.WorkerID)
		}

		for _, mapTask := range c.MapTasks {
			if mapTask.Status == IDLE {
				// fmt.Printf("Coordinator: assigning map task: %v\n", mapTask.InputFile)
				mapTask.Assign(args.WorkerID)
				*reply = mapTask
				break
			}
		}
		return nil
	}

	// check for unstarted reduce tasks
	if c.RemainingReduceTasks != 0 {
		for _, reduceTask := range c.ReduceTasks {
			reduceTask.Locations = c.Locations
			// fmt.Println("C.Location:==", c.Locations)
			// fmt.Println("R.Locations:==", reduceTask.Locations)
			if reduceTask.Status == IDLE {
				// fmt.Printf("Coordinator: assigning reduce task: %v\n", reduceTask.Region)
				reduceTask.Assign(args.WorkerID)
				*reply = reduceTask
				c.RemainingReduceTasks -= 1
				break
			}
		}
		return nil
	}

	// no more tasks
	// fmt.Println("Coordinator: No idle task found")
	reply = nil
	return nil
}

func (c *Coordinator) MarkTaskDone(args *TaskDoneArgs, reply *TaskDoneReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.RemainingMapTasks -= 1
	return nil
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()
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
		Locations:            []string{},
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
			Region: i + 1,
			Task: Task{
				Status: IDLE,
			},
		}
	}

	c.server()
	return &c
}
