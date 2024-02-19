package mr

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sort"
)

type Mapper struct {
	id          int
	nReduce     int
	filesnames  []string
	encoders    []*json.Encoder
	tmpFiles    []string
	fileHandles []*os.File
	finalFiles  []string
}

func NewMapper(task TaskReply) Mapper {
	mapper := Mapper{id: task.Id, nReduce: task.NReduce,
		filesnames: task.Filenames}
	return mapper
}

// Initialization of temporary files and encoders.
func (w *Mapper) initializeEncoders() {
	encoders := make([]*json.Encoder, w.nReduce)
	tempFiles := make([]string, w.nReduce)
	// Added slice to keep track of file handles
	fileHandles := make([]*os.File, w.nReduce)

	for i := 0; i < w.nReduce; i++ {
		fileName := fmt.Sprintf("mr-%v-%v-tmp", w.id, i)
		tempFile, err := os.CreateTemp(".", fileName)
		if err != nil {
			log.Fatalf("Failed to create temp file: %v", err)
			return
		}
		encoders[i] = json.NewEncoder(tempFile)
		tempFiles[i] = tempFile.Name()
		fileHandles[i] = tempFile // Store the file handle
	}
	w.encoders = encoders
	w.tmpFiles = tempFiles
	w.fileHandles = fileHandles
}

func (w *Mapper) emitIntermediate(kv KeyValue) {
	reduceTaskNumber := ihash(kv.Key) % w.nReduce
	err := w.encoders[reduceTaskNumber].Encode(&kv)
	if err != nil {
		log.Fatalf("Failed to encode kv: %v", err)
	}
}

// New function to close all temporary files
func (w *Mapper) closeTempFiles() {
	for _, file := range w.fileHandles {
		err := file.Close()
		if err != nil {
			log.Fatalf("Failed to close file: %v", err)
		}
	}
}

// returns slice of len(nReduce)
// slice[i] = file for reducer i
func (w *Mapper) finalizeIntermediateFiles() []string {
	s := make([]string, 0, w.nReduce)
	for i, tempFileName := range w.tmpFiles {
		// Name format: mr-X-Y
		finalName := fmt.Sprintf("mr-%v-%v", w.id, i)
		err := os.Rename(tempFileName, finalName)
		if err != nil {
			log.Fatalf("Failed to atomically rename file: %v", err)
		}
		// fmt.Printf("%v remnamed to -> %v\n", tempFileName, finalName)
		s = append(s, finalName)
		// fmt.Printf("updated array: %v\n", s)
	}
	return s
}

func Mapify(worker *Mapper, mapf func(string, string) []KeyValue, filename string) {
	intermediate := []KeyValue{}
	content := readFile(filename)

	kva := mapf(filename, content)
	intermediate = append(intermediate, kva...)

	sort.Sort(ByKey(intermediate))

	// Step 1: Initialize encoders and temporary files
	worker.initializeEncoders()

	// Step 2: Process each record and emit intermediate key-value pairs
	for _, kv := range intermediate {
		worker.emitIntermediate(kv)
	}

	worker.closeTempFiles()

	// Step 3: Finalize intermediate files by atomically renaming them
	worker.finalFiles = worker.finalizeIntermediateFiles()
}
