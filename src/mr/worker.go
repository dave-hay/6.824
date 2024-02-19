package mr

import (
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func getTasks() (TaskReply, error) {
	args := None{}
	task := TaskReply{}

	ok := callRpc("Master.GetTask", &args, &task)
	if !ok {
		return task, errors.New("error getting task")
	}
	return task, nil
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// send RPC to coordinator asking for task
	isJob := true
	for isJob {
		task, err := getTasks()
		if err != nil {
			log.Panicln(err)
			return
		}
		switch task.TaskType {
		case "map":
			mapper := NewMapper(task)
			// fmt.Println("mapper started")
			Mapify(&mapper, mapf, task.Filenames[0])
			// fmt.Println("mapper finished")
			// fmt.Printf("Id: %v, FinalFiles: %v\n", mapper.id, mapper.finalFiles)
			args := MapCompleteArg{Id: mapper.id, FinalFiles: mapper.finalFiles}
			none := None{}

			// call rpc complete map
			ok := callRpc("Master.MapTaskCompleted", &args, &none)
			if !ok {
				log.Fatalf("error completing map task for mapper %v", mapper.id)
				return
			}
		case "reduce":

			// TODO: call Reduce func, reducef
			log.Fatal("cannot call reduce worker")
		default:
			isJob = false
		}
	}
}

func readFile(filename string) string {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	return string(content)
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func callRpc(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
