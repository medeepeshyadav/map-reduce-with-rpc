package mr

import (
	"encoding/gob"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"

	"github.com/google/uuid"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type WorkerClass struct {
	WorkerID        string
	RegionToKVPairs map[int][]KeyValue
}

type MapReduceTaskReply interface{}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	// Setting up worker server
	workerID := uuid.New().String()[:6]
	workerObj := WorkerClass{
		WorkerID:        workerID,
		RegionToKVPairs: make(map[int][]KeyValue),
	}
	gob.Register(MapTask{})
	gob.Register(ReduceTask{})
	args := GetTaskArgs{WorkerID: workerID}
	reply := new(MapReduceTaskReply)
	// initial call to coordinator GetTask method
	ok := call(coordinatorSock(), "Coordinator.GetTask", &args, &reply)
	for ok && *reply != nil {
		switch task := (*reply).(type) {
		case MapTask:
			// fmt.Printf("Processing map task: %v\n", task)
			ok = workerObj.handleMapTask(mapf, task.InputFile, task.NReduce)
			call(coordinatorSock(), "Coordinator.MarkTaskDone", &TaskDoneArgs{}, &TaskDoneReply{})

		case ReduceTask:
			// fmt.Printf("Processing reduce task: %v\n", task)
			ok = workerObj.handleReduceTask(reducef, task.Region, task.Locations)
		default:
			fmt.Printf("Unknown task type %T, terminating program.\n", task)
			ok = false
		}
		// time.Sleep(2 * time.Second)
		ok = call(coordinatorSock(), "Coordinator.GetTask", &args, &reply)
	}

}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(sockName string, rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", sockName)
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

// builds a unique sockname using the worker id
func (w *WorkerClass) Sockname() string {
	return fmt.Sprintf("/var/tmp/5840-mr-%v", w.WorkerID)
}

// handles map tasks
func (w *WorkerClass) handleMapTask(mapf func(string, string) []KeyValue, fileName string, NReduce int) bool {
	// open the file
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	defer file.Close()
	// read file contents
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}
	// send the read content to Map function to
	// get intermediate key value pairs
	listOfKeyValue := mapf(fileName, string(content))

	// partitioning
	for _, kvPair := range listOfKeyValue {
		region := ihash(kvPair.Key)%NReduce + 1
		w.RegionToKVPairs[region] = append(w.RegionToKVPairs[region], kvPair)
	}

	// store the intermediate k-v pairs in json file
	for region, intermediateKVPairs := range w.RegionToKVPairs {
		intermediateFileName := fmt.Sprintf("%v-%d", w.WorkerID, region)
		intermediateFile, err := os.Create(intermediateFileName)
		// w.Locations[region] = append(w.Locations[region], w.WorkerID)

		if err != nil {
			log.Fatalf("could not create %v", intermediateFileName)
		}

		// initialize JSON encoder
		encoder := json.NewEncoder(intermediateFile)
		for _, kvPair := range intermediateKVPairs {
			err := encoder.Encode(kvPair)
			if err != nil {
				log.Fatal(err)
			}
		}
		intermediateFile.Close()
	}
	return true
}

// handles reduce task
func (w *WorkerClass) handleReduceTask(reducef func(string, []string) string, region int, locations []string) bool {
	oname := fmt.Sprintf("mr-out-%d", region)
	ofile, err := os.Open(oname)
	if os.IsNotExist(err) {
		ofile, _ = os.Create(oname)
	}
	var listOfKeyValue []KeyValue
	for _, workerID := range locations {
		intermediateFileName := fmt.Sprintf("%v-%d", workerID, region)
		file, err := os.Open(intermediateFileName)

		if err != nil {
			log.Fatalf("error occured while opening %v file, Error:%v", intermediateFileName, err)
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			listOfKeyValue = append(listOfKeyValue, kv)
		}

		file.Close()
	}
	sort.Sort(ByKey(listOfKeyValue))

	//
	// call Reduce on each distinct key in listOfKeyValue[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(listOfKeyValue) {
		j := i + 1
		for j < len(listOfKeyValue) && listOfKeyValue[j].Key == listOfKeyValue[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, listOfKeyValue[k].Value)
		}
		output := reducef(listOfKeyValue[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", listOfKeyValue[i].Key, output)

		i = j
	}
	ofile.Close()
	return true
}
