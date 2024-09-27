package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
// key := KeyValue.Key 
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// implements the sort.Interface interface in order to use sort.Sort()
//
type ByKey []KeyValue

func (s ByKey) Len() int {return len(s)}
func (s ByKey) Swap(i, j int) {s[i], s[j] = s[j], s[i]}
func (s ByKey) Less(i, j int) bool {return s[i].Key < s[j].Key}



//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// ask for task 
	for {
		args := TaskRequestArgs{}
		reply := TaskRequestReply{}
		ok := call("Coordinator.Request_Task", &args, &reply)

		if !ok {
			// had problems with contacting the coordinator, exit the program
			err := fmt.Errorf("current worker had problems with contacting with the coordinator, worker exited")
			log.Println(err)
			break
		}

		switch reply.Task.TaskType {
		case MapTaskType:
			// do the map task 
			doMapTask(reply.Task, mapf)
			// report task completion 
			reportTaskCompletion(MapTaskType, reply.Task.TaskId)
		case ReduceTaskType:
			// do the reduce task 
			doReduceTask(reply.Task, reducef)
			// report task completion 
			reportTaskCompletion(ReduceTaskType, reply.Task.TaskId)
		case WaitTaskType:
			// sleep and retry 
			time.Sleep(3 * time.Second)
		case ExitTaskType:
			// job is done; exit 
			return 
		default:
			// unrecognized task type, exit 
			return 
		}


	}
	
}

// word count: for each input record, call a map function
// the task itself returns nothing
func doMapTask(task Task, mapf func(string, string) []KeyValue) {
	// read the input file 
	filename := task.Filename
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	// a slice of KetValue pairs 
	kva := mapf(filename, string(content))

	// partition kva into nReduce intermediate files 
	nReduce := task.NReduce
	intermediateFiles := make([]*os.File, nReduce)
	encoders := make([]*json.Encoder, nReduce)

	// create temp files and encoders 
	for i:= 0; i < nReduce; i++ {
		intermediateFilename := fmt.Sprintf("mr-%d-%d", task.TaskId, i)
		tempFile, err := os.CreateTemp("", intermediateFilename)
		if err != nil {
			log.Fatalf("cannot create temp file %v", intermediateFilename)
		}
		intermediateFiles[i] = tempFile
		// returns an encoder writes to tempFile, and we need nReduce difference encoders to 
		// write to nReduce different intermediate files 
		encoders[i] = json.NewEncoder(tempFile)
	}

	// partition key value pairs 
	for _, kv := range kva {
		reduceTaskId := ihash(kv.Key) % nReduce
		// encoder each key value pair
		err := encoders[reduceTaskId].Encode(&kv)
		if err != nil {
			log.Fatalf("cannot encoder kv pair")
		}
	}

	// close and rename temp files to avoid partially written files is observed in presence of crashes 
	for i := 0; i < nReduce; i ++ {
		tempFile := intermediateFiles[i]
		intermediateFileName := fmt.Sprintf("mr-%d-%d", task.TaskId, i)
		tempName := tempFile.Name()
		tempFile.Close()
		// rename the file atomically after the job for this intermediate file is done
		os.Rename(tempName, intermediateFileName)
	}
}


// word count: call one reduce function for each distinct key
// the task itself returns nothing 
func doReduceTask(task Task, reducef func(string, []string) string) {
	nMap := task.NMap
	intermediate := []KeyValue{}

	// Read intermediate files from all map tasks 
	for i := 0; i < nMap; i ++ {
		intermediateFileName := fmt.Sprintf("mr-%d-%d", i, task.TaskId)
		file, err := os.Open(intermediateFileName)
		if err != nil {
			log.Fatal("cannot open intermediate file %v\n", intermediateFileName)
		}
		decoder := json.NewDecoder(file)

		// read the file into intermediate []KeyValue 
		for {
			var kv KeyValue
			if err := decoder.Decode(&kv); err != nil {
				if err == io.EOF{
					// read till the end
					break
				}
				log.Fatalf("cannot decode a kv pair")
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	// sort them 
	sort.Sort(ByKey(intermediate)) 

	// create output files 
	oname := fmt.Sprintf("mr-out-%d", task.TaskId)
	tempFile, err := os.CreateTemp("", oname)
	if err != nil {
		log.Fatalf("cannot create temp file %v", oname)
	}

	// call the reduce function 
	i := 0 
	for i < len(intermediate){
		j := i + 1 // exclusive
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j ++
		}
		var values []string 
		for k := i; k < j; k ++ {
			values = append(values, intermediate[k].Value)
		}
		// call reduce function for this key, the output is a string 
		output := reducef(intermediate[i].Key, values)

		// write the output to the file 
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}


	// close and rename temp file 
	tempName := tempFile.Name()
	tempFile.Close()
	os.Rename(tempName, oname)

}

func reportTaskCompletion(taskType TaskType, taskId int) error {

	for i := 0; i < 3; i ++ {
		args := ReportTaskArgs{
			TaskType: taskType,
			TaskId: taskId,
		}
		reply := ReportTaskReply{}
		ok := call("Coordinator.Report_Task", &args, &reply)
		if ok {
			// break the loop and the completion report succeeds 
			log.Printf("Task %d reported succcessfully\n", taskId)
			return nil
		}
	}
	
	// Tried three times and all three times failed
	return fmt.Errorf("failed to report task completion for task %d after 3 attempts", taskId)
}



// call sends an RPC request to the coordinator and wairs for the response
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