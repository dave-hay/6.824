package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

// General
type None struct{}

// MapTaskComplete
type MapCompleteArg struct {
	Id         int
	FinalFiles []string
}

type TaskCompleteArgs struct {
	Id         int
	FinalFiles []string
	Type       string
}

// GetTask
type TaskReply struct {
	Id        int
	TaskType  string
	NReduce   int
	Filenames []string
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
