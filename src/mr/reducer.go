package mr

import (
	"fmt"
	"log"
	"os"
	"sync"
)

var (
	// intermediate []KeyValue
	// lock   sync.Mutex
	rwLock sync.RWMutex
)

type Reducer struct {
	id         int
	nReduce    int
	filesnames []string
}

func NewReducer(task TaskReply) Reducer {
	reducer := Reducer{id: task.Id, nReduce: task.NReduce,
		filesnames: task.Filenames}
	return reducer
}

func Reduce(worker *Reducer, reducef func(string, []string) string) {
	oname := fmt.Sprintf("./out/mr-out-%v", worker.id)
	ofile, err := os.Create(oname)
	if err != nil {
		log.Fatalf("Failed to create output file: %v", err)
		return
	}
	defer ofile.Close()

	// intermediate := []KeyValue{}

	var wg sync.WaitGroup
	// for file in dir
	// Assume `readFile` returns ([]KeyValue, error) now, handling error appropriately
	// content := readFile(file)

	for _, file := range worker.filesnames {
		wg.Add(1)
		go func(f string) {
			// TODO: read file
			rwLock.RLock() // Acquire read lock
			fmt.Println(f)
			rwLock.RUnlock() // Release read lock
		}(file)
	}

	if err != nil {
		log.Fatalf("Error walking through directory: %v", err)
	}

	wg.Wait()
}
