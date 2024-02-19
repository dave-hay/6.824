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
			defer wg.Done()
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

// func readIntermediate() {
// 	ff := func(r rune) bool { return !unicode.IsLetter(r) }
// 	kvCount := map[string]int

// 	// split contents into an array of words.
// 	words := strings.FieldsFunc(contents, ff)

// 	kva := []KeyValue{}
// 	for _, w := range words {
// 		kv := KeyValue{w, "1"}
// 		kva = append(kva, kv)
// 	}
// 	return kva
// }

// func accumulateWordCounts(filePath string) (map[string]int, error) {
// 	file, err := os.Open(filePath)
// 	if err != nil {
// 		return nil, err
// 	}
// 	defer file.Close()

// 	wordCounts := make(map[string]int)
// 	scanner := bufio.NewScanner(file)
// 	for scanner.Scan() {
// 		line := scanner.Text()
// 		parts := strings.Split(line, ": ")
// 		if len(parts) != 2 {
// 			continue // Skip lines that don't match the expected format
// 		}

// 		word := parts[0]
// 		countIncrement, err := strconv.Atoi(parts[1])
// 		if err != nil {
// 			// If the count is not an integer, log the error and continue with the next line
// 			fmt.Printf("Warning: invalid count for word '%s', assuming 1\n", word)
// 			countIncrement = 1 // Default increment to 1 if conversion fails
// 		}

// 		// Increment the count for the word in the map
// 		wordCounts[word] += countIncrement
// 	}

// 	if err := scanner.Err(); err != nil {
// 		return nil, err
// 	}

// 	return wordCounts, nil
// }

// func reducer() {

// 	sort.Sort(ByKey(intermediate))

// 	for i := 0; i < len(intermediate); {
// 		j := i + 1
// 		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
// 			j++
// 		}
// 		values := make([]string, j-i)
// 		for k := i; k < j; k++ {
// 			values[k-i] = intermediate[k].Value
// 		}
// 		output := reducef(intermediate[i].Key, values)
// 		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
// 		i = j
// 	}
// }
