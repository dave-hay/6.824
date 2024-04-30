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
	rf.persist()
	rf.mu.Unlock()

	for i := prevIndex + 1; i <= index; i++ {
		rf.logQueue.indexes = append(
			rf.logQueue.indexes,
			i,
		)
	}

	rf.logQueue.cond.Signal()
}

func (rf *Raft) applyLogs() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		msg := ApplyMsg{
			CommandValid: true,
			Command:      rf.logs[i-1].Command,
			CommandIndex: i,
		}
		rf.applyCh <- msg
		rf.lastApplied = i
	}
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
