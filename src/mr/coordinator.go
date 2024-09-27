package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// represents the state of a task
type TaskState string 

/*
	Two state change situations: 
	Idle -> InProgress: once a task is assigned 
	InProgress -> Completed: once a task is finished, a rpc handler will deal with the report from the worker 
*/

const (
	Idle 		TaskState = "Idle"
	InProgress 	TaskState = "InProgress"
	Completed 	TaskState = "Completed"
)


// MapTask represents a Map task with its metadata 
type MapTask struct {
	TaskId 			int 
	State 			TaskState 
	Filename 		string 
	NReduce 		int
	AssignedTime	time.Time
}

// ReduceTask represents a Reduce Task with its metadata 
type ReduceTask struct {
	TaskId 			int 
	State 			TaskState 
	NMap 			int
	AssignedTime	time.Time
}



/*
"for each map task and reduce task, it stores the state and the identity of the work machine (for non-idle tasks)"
assign idle map tasks to worker nodes 
The coordinator is a RPC server and will be concurrent, lock the shared data!
*/

type Coordinator struct {
	mu 				sync.Mutex
	mapTasks 		[]MapTask 
	reduceTasks 	[]ReduceTask
	nReduce 		int 
	nMap 			int

}

// RPC handlers for the worker to call.


// Request_Task handles task requests from workers 
func (c *Coordinator) Request_Task(args *TaskRequestArgs, reply *TaskRequestReply) error {

	c.mu.Lock()
	defer c.mu.Unlock()

	
	// check if there are more map tasks to assign
	allMapTasksCompleted := true 
	for i, task := range c.mapTasks {
		switch task.State {

		case Idle:
			// Assign Idle Map Task 
			c.mapTasks[i].State = InProgress
			c.mapTasks[i].AssignedTime = time.Now()
			reply.Task = Task{
				TaskType: MapTaskType,
				TaskId: task.TaskId,
				Filename: task.Filename,
				NReduce: c.nReduce,
			}
			return nil

		case InProgress:
			// check for timeout (10 seconds)
			// Backup Task
			if time.Since(task.AssignedTime) > 10 * time.Second {
				// reassign task 
				c.mapTasks[i].AssignedTime = time.Now()
				reply.Task = Task{
					TaskType: MapTaskType,
					TaskId: task.TaskId,
					Filename: task.Filename,
					NReduce: c.nReduce,
				}
				return nil
			}
			// if within 10 seconds, simply make the flag as False 
			allMapTasksCompleted = false 
	
		case Completed:
			// Do nothing
		}
	}

	// if all tasks are assigned, no timeout and no idle tasks, just tell the worker to wait 
	if !allMapTasksCompleted {
		reply.Task = Task{
			TaskType: WaitTaskType,
		}
		return nil
	}


	// if all map tasks are complete, enter the reduce phase 
	allReduceTasksCompleted := true 
	for i, task := range c.reduceTasks {
		switch task.State {
		case Idle:
			// Assign Idle reduce tasks 
			c.reduceTasks[i].AssignedTime = time.Now()
			c.reduceTasks[i].State = InProgress
			reply.Task = Task { // ???????how about those unfilled fields?????
				TaskType: ReduceTaskType,
				TaskId: task.TaskId,
				NMap: c.nMap,
			}
			return nil

		case InProgress:
			// check time out 
			if time.Since(task.AssignedTime) > 10 * time.Second {
				// reassign it 
				c.reduceTasks[i].AssignedTime = time.Now()
				reply.Task = Task {
					TaskType: ReduceTaskType,
					TaskId: task.TaskId,
					NMap: c.nMap,
				}
				return nil
			}
			allReduceTasksCompleted = false

		case Completed:
			// Do nothing
		}
	}

	if !allReduceTasksCompleted {
		reply.Task = Task {
			TaskType: WaitTaskType,
		}
		return nil
	}

	// all map and reduce tasks are complete, instructor workers to exit
	reply.Task = Task {
		TaskType: ExitTaskType,
	}
	return nil
}


// Report_Task handles task complete reports from workers
func (c *Coordinator) Report_Task(args *ReportTaskArgs, reply *ReportTaskReply) error {

	c.mu.Lock()
	defer c.mu.Unlock()

	if args.TaskType == MapTaskType {
		if c.mapTasks[args.TaskId].State == InProgress {
			c.mapTasks[args.TaskId].State = Completed
		} else {
			// raise an error
			return errors.New("only tasks in inprogress state could be completed")
		}
	} else if args.TaskType == ReduceTaskType {
		if c.reduceTasks[args.TaskId].State == InProgress {
			c.reduceTasks[args.TaskId].State = Completed
		} else {
			// raise an error
			return errors.New("only tasks in inprogress state could be completed")
		}
	}
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname) // prevent address already in use error
	// the Listen function creates servers 
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	
	c.mu.Lock() // ???? what on earth is this locking? the entire c? or just tasks??????
	defer c.mu.Unlock()

	for _, task := range c.mapTasks {
		if task.State != Completed {
			return false
		}
	}

	for _, task := range c.reduceTasks {
		if task.State != Completed {
			return false
		}
	}

	return true
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	
	// while initializing tasks, taskId should equal the task index in the coordinator
	// the number of map tasks equal to the number of files

	c := Coordinator{
		mapTasks: 			make([]MapTask, len(files)),
		reduceTasks: 		make([]ReduceTask, nReduce),
		nReduce: 			nReduce,
		nMap: 				len(files),
	}

	// Initialize Map Tasks 
	for i := 0; i < len(files); i++ {
		c.mapTasks[i] = MapTask {
			TaskId: 		i,
			State: 			Idle,
			Filename: 		files[i],
			NReduce: 		nReduce,
		}
	}

	// Initialize Reduce Tasks 
	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = ReduceTask{
			TaskId: 		i,
			State: 			Idle,
			NMap: 			len(files),
		}
	}
	
	c.server()
	return &c
}