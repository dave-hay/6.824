package raft

// apply()
// applies command to state machine
func (rf *Raft) apply(nextIndex int, leaderLogs []LogEntry) {
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	newLogs := leaderLogs[nextIndex:]
	DPrint(rf.me, "apply()", "current logs: %v; new logs: %v", rf.logs, newLogs)

	// next index
	if nextIndex <= rf.lastApplied {
		return
	}

	// append new entries not already in the log
	rf.logs = append(rf.logs, newLogs...)

	msg := ApplyMsg{
		CommandValid: true,
		Command:      rf.logs[nextIndex].Command,
		CommandIndex: nextIndex,
	}

	rf.lastApplied = nextIndex

	rf.applyCh <- msg
}
