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
	DPrint(rf.me, "Start()", "command: %v", command)
	index := -1
	term := -1
	isLeader := false

	if rf.getState() != Leader {
		return index, term, isLeader
	}

	rf.mu.Lock()
	term = rf.currentTerm
	rf.logs = append(rf.logs, LogEntry{Term: term, Command: command}) // append command to log
	index = len(rf.logs) - 1
	isLeader = rf.state == Leader
	rf.mu.Unlock()

	// fire off AppendEntries
	go rf.sendLogEntries()

	return index, term, isLeader
}