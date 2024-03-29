package raft

type leaderState struct {
	// volatile state, leaders
	// tracking the state of other servers

	// index of the next log entry to send to each server
	// initialized to leader last log index + 1
	nextIndex []int

	// index of highest log entry know to be replicated on each server
	// initialized to 0, increasing monotonically
	matchIndex []int
}

func (rf *Raft) sendAppendEntries(server int, isHeartbeat bool) {
	rf.mu.Lock()
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: len(rf.log),
		PrevLogTerm:  len(rf.log),
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()

	if isHeartbeat {
		args.Entries = make([]Log, 0)
	}

	reply := &AppendEntriesReply{}

	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	if !ok {
		return
	}

	if reply.Success {
		// TODO: Update nextIndex and matchIndex for the follower
		// TODO: Check if we can commit new entries
	} else if reply.Term > rf.currentTerm {
		rf.convertToFollower()
	}
}

func (rf *Raft) sendHeartbeats() {
	for server := range len(rf.peers) {
		if server != rf.me {
			go rf.sendAppendEntries(server, true)
		}
	}
}

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
