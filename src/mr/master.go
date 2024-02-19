package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Master struct {
	nReduce     int
	mapTasks    map[int]Task
	reduceTasks map[int]Task
}

type Task struct {
	id        int
	nReduce   int
	filenames []string
	inProcess bool
	taskType  string
	// isComplete bool // when done just delete?
}

// Your code here -- RPC handlers for the worker to call.
func (c *Master) GetTask(args *None, reply *TaskReply) error {
	mapTasks, reduceTasks := len(c.mapTasks), len(c.reduceTasks)
	fmt.Println()
	fmt.Println("Called GetTask")
	fmt.Printf("mapTasks: %v, reduceTasks: %v\n", mapTasks, reduceTasks)
	fmt.Println()

	if mapTasks == 0 && reduceTasks == 0 {
		return nil
	}

	if mapTasks == 0 {
		for _, task := range c.reduceTasks {
			if !task.inProcess {
				reply.TaskType = "reduce"
				reply.Filenames = task.filenames
				reply.NReduce = task.nReduce
				reply.Id = task.id
				task.inProcess = true
				break
			}
		}
	} else {
		for _, task := range c.mapTasks {
			if !task.inProcess {
				reply.TaskType = task.taskType
				reply.Filenames = task.filenames
				reply.NReduce = task.nReduce
				reply.Id = task.id
				task.inProcess = true
				break
			}
		}
	}

	return nil
}

func (c *Master) TaskComplete(args *TaskCompleteArgs, reply *None) error {
	fmt.Println()
	fmt.Println("TaskComplete called")
	fmt.Printf("Task info: %v\n", args)
	fmt.Println()

	if args.Type == "reduce" {

		delete(c.reduceTasks, args.Id)

	} else if args.Type == "map" {
		delete(c.mapTasks, args.Id)

		// add filenames to reduce objs
		for i, file := range args.FinalFiles {
			task := c.reduceTasks[i]
			task.filenames = append(task.filenames, file)
			c.reduceTasks[i] = task
			// fmt.Printf("task %v, file: %v", i, c.reduceTasks[i].filenames)
		}
	}

	return nil
}

func (c *Master) MapTaskCompleted(args *MapCompleteArg, reply *None) error {
	// delete mapTask from c
	// fmt.Println("MapTaskCompleted called")
	delete(c.mapTasks, args.Id)

	// add filenames to reduce objs
	for i, file := range args.FinalFiles {
		task := c.reduceTasks[i]
		task.filenames = append(task.filenames, file)
		c.reduceTasks[i] = task
		// fmt.Printf("task %v, file: %v", i, c.reduceTasks[i].filenames)
	}
	return nil
}

// todo: if map task error set inProcess to false
func (c *Master) MapTaskError() error {
	return nil
}

// main/mrMaster.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Master) Done() bool {
	return len(c.mapTasks) == 0 && len(c.reduceTasks) == 0
}

// create a Master.
// main/mrMaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	mapTasks := map[int]Task{}
	reduceTasks := map[int]Task{}

	for i, file := range files {
		mapTasks[i] = Task{id: i, taskType: "map", filenames: []string{file}, nReduce: nReduce}
	}

	for i := range nReduce {
		reduceTasks[i] = Task{id: i, taskType: "reduce", filenames: []string{}, nReduce: nReduce}
	}

	c := Master{nReduce: nReduce, mapTasks: mapTasks, reduceTasks: reduceTasks}

	c.server()
	return &c
}

// start a thread that listens for RPCs from worker.go
func (c *Master) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}
