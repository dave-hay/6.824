package raft

import "time"

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

	// reset so leader keeps authority
	rf.lastHeardFromLeader = time.Now()
	rf.votedFor = -1
	rf.state = Follower

	// If leader's prevLogTerm != follower's prevLogTerm:
	// reply false and delete all existing entries from prevLogIndex forward
	if args.LeaderPrevLogIndex <= curPrevLogIndex &&
		args.LeaderPrevLogTerm != rf.logs[args.LeaderPrevLogIndex].Term {
		rf.logs = rf.logs[:args.LeaderPrevLogIndex-1]
		reply.Success = false
		return
	}

	reply.Success = true

	// if empty LogEntires it is a heartbeat
	if len(args.LeaderLogEntries) > 0 {
		// TODO: append new entries not already in the log
		DPrintln("append new entries not already in the log")
	}

	// update commitIndex with highest known log entry
	rf.commitIndex = min(args.LeaderCommitIndex, curPrevLogIndex)

}

// sendAppendEntries method
// server (int) defines serverId RPC is for
// if isHeartbeat (bool) is true sets logEntries to empty array
func (rf *Raft) sendAppendEntries(server int, isHeartbeat bool) {
	rf.mu.Lock()

	args := &AppendEntriesArgs{
		LeaderTerm:         rf.currentTerm,
		LeaderId:           rf.me,
		LeaderPrevLogIndex: len(rf.logs) - 1,
		LeaderPrevLogTerm:  rf.logs[len(rf.logs)-1].Term,
		LeaderLogEntries:   rf.logs,
		LeaderCommitIndex:  rf.commitIndex,
	}

	if isHeartbeat {
		args.LeaderLogEntries = make([]LogEntry, 0)
	}

	reply := &AppendEntriesReply{}
	rf.mu.Unlock()

	// unlocked while processing
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	rf.mu.Lock()

	if !ok {
		// TODO: retry; exponential backoff
		return
	}

	// convert to follower
	if !reply.Success && reply.Term > args.LeaderTerm {
		rf.currentTerm = reply.Term
		rf.lastHeardFromLeader = time.Now()
		rf.votedFor = -1
		rf.state = Follower
	}
	rf.mu.Unlock()
}

// sendHeartbeats method
// triggered by leader sending empty AppendEntries RPCs to followers
func (rf *Raft) sendHeartbeats() {
	for serverId := range len(rf.peers) {
		if serverId != rf.me {
			go rf.sendAppendEntries(serverId, true)
		}
	}

}