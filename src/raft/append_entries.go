package raft

// Implements the AppendEntries RPC handler and related functions.
// Handles the logic for appending entries to the log and
// replicating them to followers.

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
	} else if reply.Term > args.Term {
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
