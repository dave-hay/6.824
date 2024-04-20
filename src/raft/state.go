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
	rf.persist()

	for i := prevIndex + 1; i <= index; i++ {
		rf.logQueue.indexes = append(
			rf.logQueue.indexes,
			i,
		)
	}

	rf.logQueue.cond.Signal()
	// DPrint(rf.me, "logQueueProducer()", "appended commit at index: %v; indexes: %v", index, rf.logQueue.indexes)
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

		// DPrint(rf.me, "logQueueConsumer()", "sending ApplyMsg to applyCh for index=%d", index)

		rf.applyCh <- msg
	}
}

type NewLogQueue struct {
	entries int // where logs applied
	cond    *sync.Cond
}

// newLogProducer
func (rf *Raft) newLogProducer() {
	rf.newLogQ.cond.L.Lock()
	defer rf.newLogQ.cond.L.Unlock()

	rf.newLogQ.entries++

	rf.newLogQ.cond.Signal()
	// DPrint(rf.me, "newLogProducer()", "called; entries: %d", rf.newLogQ.entries)
}

// newLogConsumer
func (rf *Raft) newLogConsumer() {
	for !rf.killed() {
		rf.newLogQ.cond.L.Lock()
		for rf.newLogQ.entries == 0 {
			rf.newLogQ.cond.Wait()
		}
		rf.newLogQ.entries--

		rf.sendLogEntries()
		rf.newLogQ.cond.L.Unlock()

		// DPrint(rf.me, "newLogConsumer()", "finished entry=%d", rf.newLogQ.entries)

	}
}
