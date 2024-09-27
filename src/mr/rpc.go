package mr

//
// RPC definitions for communication between the workers and the coordinator
//
//
//

import (
	"os"
	"strconv"
)

// TaskType represents the type of task: Map, Reduce, Wait, or Exit
type TaskType int 

const (
	MapTaskType TaskType = iota
	ReduceTaskType 
	WaitTaskType 
	ExitTaskType
)

// Task holds information about a task assigned to a worker 
type Task struct {
	TaskType TaskType
	TaskId int 
	Filename string 
	NReduce int 
	NMap int
}


// TaskRequestArgs represents the arguments for a task request RPC
type TaskRequestArgs struct {

}


// TaskRequestReply represents the reply from the coordinator to a task request 
type TaskRequestReply struct {
	Task Task 
}

// ReportTaskArgs represents the arguments for reporting task completion 
type ReportTaskArgs struct {
	TaskType TaskType 
	TaskId int
}

// represents the reply to a task completion report 
type ReportTaskReply struct {

}


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}