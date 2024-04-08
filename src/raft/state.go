package raft

import (
	"sync"
)

// keeps order of applied logs
type LogQueue struct {
	indexes []int // where logs applied
	cond    *sync.Cond
}

// logQueueProducer
// when applying new entries
func (rf *Raft) logQueueProducer(index int) {
	rf.logQueue.cond.L.Lock()
	defer rf.logQueue.cond.L.Unlock()

	rf.mu.Lock()
	prevIndex := rf.lastApplied
	rf.mu.Unlock()

	for i := prevIndex + 1; i <= index; i++ {
		rf.logQueue.indexes = append(
			rf.logQueue.indexes,
			i,
		)
	}

	rf.logQueue.cond.Signal()
	DPrint(rf.me, "logQueueProducer()", "appended commit at index: %v; indexes: %v", index, rf.logQueue.indexes)
}

// logQueueConsumer
// creates + sends ApplyMsg to applyCh
func (rf *Raft) logQueueConsumer() {
	for !rf.killed() {
		rf.logQueue.cond.L.Lock()
		for len(rf.logQueue.indexes) == 0 {
			rf.logQueue.cond.Wait()
		}

		index := rf.logQueue.indexes[0]
		rf.logQueue.indexes = rf.logQueue.indexes[1:]

		rf.logQueue.cond.L.Unlock()

		rf.mu.Lock()
		msg := ApplyMsg{
			CommandValid: true,
			Command:      rf.logs[index-1].Command,
			CommandIndex: index,
		}
		rf.lastApplied = index
		rf.mu.Unlock()

		DPrint(rf.me, "logQueueConsumer()", "sending ApplyMsg to applyCh")

		rf.applyCh <- msg
	}
}

// Start()
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log.
// start the agreement and return immediately.
// no guarantee command will be committed to the Raft log
//
// index (int): index the command will appear if commited. len(rf.logs) + 1
// term (int): current term
// isLeader (bool): if server is leader
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false

	if rf.getState() != Leader {
		// DPrint(rf.me, "Start()", "rejected is not leader")
		return index, term, isLeader
	}

	rf.mu.Lock()
	term = rf.currentTerm
	isLeader = rf.state == Leader
	index = len(rf.logs) + 1
	rf.logs = append(rf.logs, LogEntry{Term: term, Command: command}) // append command to log
	// DPrint(rf.me, "Start()", "command: %v appended to log; index: %d; logs: %v", command, index, rf.logs)
	rf.mu.Unlock()

	// fire off AppendEntries
	go rf.sendLogEntries()

	return index, term, isLeader
}
