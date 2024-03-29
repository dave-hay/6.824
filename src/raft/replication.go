package raft

// Start agreement to append command to replicated log
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false

	// if not leader return
	// rf.mu.Lock()
	// if rf.state == Leader {
	// 	index = rf.commitIndex
	// 	term = rf.currentTerm
	// 	isLeader = true
	// 	return index, term, isLeader
	// }
	// rf.mu.Unlock()

	return index, term, isLeader
}
