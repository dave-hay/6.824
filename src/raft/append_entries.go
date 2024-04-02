package raft

type AppendEntriesArgs struct {
	LeaderTerm         int
	LeaderId           int
	LeaderPrevLogIndex int
	LeaderPrevLogTerm  int
	LeaderLogEntries   []LogEntry
	LeaderCommitIndex  int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// AppendEntries RPC
// called by follower invoked by current leader
// for replicating log entries + heartbeats
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// if leader term < follower term; leader becomes follower
	if args.LeaderTerm < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	curPrevLogIndex := len(rf.logs) - 1

	// If leader's prevLogTerm != follower's prevLogTerm:
	// reply false and delete all existing entries from prevLogIndex forward
	if args.LeaderPrevLogIndex <= curPrevLogIndex &&
		args.LeaderPrevLogTerm != rf.logs[args.LeaderPrevLogIndex].Term {
		rf.logs = rf.logs[:args.LeaderPrevLogIndex-1]
		reply.Success = false
		return
	}

	// if empty LogEntires it is a heartbeat
	if len(args.LeaderLogEntries) > 0 {
		// TODO: append new entries not already in the log
	}

	// update commitIndex with highest known log entry
	rf.commitIndex = min(args.LeaderCommitIndex, curPrevLogIndex)

}
