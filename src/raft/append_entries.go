package raft

import "time"

type AppendEntriesArgs struct {
	LeaderTerm         int
	LeaderId           int
	LeaderPrevLogIndex int // nextIndex[followerId]
	LeaderPrevLogTerm  int
	LeaderLogEntries   []LogEntry
	LeaderCommitIndex  int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// makeAppendEntriesArgs
// uses nextIndex[server] - 1 for prev log index && term
func (rf *Raft) makeAppendEntriesArgs(server int) *AppendEntriesArgs {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	args := &AppendEntriesArgs{
		LeaderTerm:        rf.currentTerm,
		LeaderId:          rf.me,
		LeaderCommitIndex: rf.commitIndex,
		// below only necessary for appending logs
		LeaderPrevLogIndex: rf.nextIndex[server] - 1,
		LeaderPrevLogTerm:  rf.logs[rf.nextIndex[server]-1].Term,
		LeaderLogEntries:   rf.logs,
	}
	return args
}

// AppendEntries RPC
// called by follower invoked by current leader
// for replicating log entries + heartbeats
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("raft %d: called AppendEntries invoked by %d", rf.me, args.LeaderId)

	// let leader know it is behind if their term < instances
	if args.LeaderTerm < rf.currentTerm {
		DPrintf("AppendEntries %d ->: %d leader behind current; ", args.LeaderId, rf.me)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// reset follower so leader keeps authority
	rf.lastHeardFromLeader = time.Now()
	rf.votedFor = -1
	rf.state = Follower
	rf.currentTerm = args.LeaderTerm
	reply.Success = true

	// if heartbeat; update commitIndex and return
	if len(args.LeaderLogEntries) == 0 {
		rf.commitIndex = max(rf.commitIndex, args.LeaderCommitIndex)
		return
	}

	// If leader's prevLogTerm != follower's prevLogTerm:
	// reply false and delete all existing entries from prevLogIndex forward
	if args.LeaderPrevLogIndex <= len(rf.logs)-1 &&
		args.LeaderPrevLogTerm != rf.logs[args.LeaderPrevLogIndex].Term {
		rf.logs = rf.logs[:args.LeaderPrevLogIndex]
		reply.Success = false
		return
	}

	// append new entries not already in the log
	rf.logs = append(rf.logs, args.LeaderLogEntries...)

	// update commitIndex with highest known log entry
	if args.LeaderCommitIndex > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommitIndex, len(rf.logs)-1)
	}

	DPrintf("AppendEntries %d ->: %d converted to follower;\n", args.LeaderId, rf.me)
}

// sendAppendEntry method
// single non-heartbeat AppendEntries RPC
// server (int) defines serverId RPC is for
func (rf *Raft) sendAppendEntry(server int) {

	for !rf.killed() {
		args := rf.makeAppendEntriesArgs(server)
		reply := &AppendEntriesReply{}

		DPrintf("raft %d: called sendAppendEntries -> %d\n", rf.me, server)
		ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
		DPrintf("raft %d: received sendAppendEntries <- %d\n", rf.me, server)

		if !ok {
			continue
		}

		if reply.Success {
			break
		}

		// convert to follower; dont retry?
		if reply.Term > args.LeaderTerm {
			DPrintf("raft %d: sendAppendEntries converted to follower\n", rf.me)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			rf.currentTerm = reply.Term
			rf.lastHeardFromLeader = time.Now()
			rf.votedFor = -1
			rf.state = Follower
			break
		}

		rf.nextIndex[server]--
		time.Sleep(10 * time.Millisecond)
	}
}

// sendLogEntries
func (rf *Raft) sendLogEntries() {
	// TODO: determine qorum of logs sent to finalize commit
	for serverId := range len(rf.peers) {
		if serverId != rf.me {
			go rf.sendAppendEntry(serverId)
		}
	}
}

// sendHeartbeat method: sends single heartbeat
// server (int) defines serverId RPC is for
func (rf *Raft) sendHeartbeat(server int) {

	args := rf.makeAppendEntriesArgs(server)
	args.LeaderLogEntries = make([]LogEntry, 0)
	reply := &AppendEntriesReply{}

	DPrintf("raft %d: called sendHeartbeat -> %d\n", rf.me, server)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	DPrintf("raft %d: received sendHeartbeat <- %d\n", rf.me, server)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// convert to follower
	if ok && !reply.Success && reply.Term > args.LeaderTerm {
		DPrintf("raft %d: sendAppendEntries converted to follower\n", rf.me)
		rf.currentTerm = reply.Term
		rf.lastHeardFromLeader = time.Now()
		rf.votedFor = -1
		rf.state = Follower
	}
}

// sendHeartbeats method
// triggered by leader sending empty AppendEntries RPCs to followers
func (rf *Raft) sendHeartbeats() {
	for serverId := range len(rf.peers) {
		if serverId != rf.me {
			go rf.sendHeartbeat(serverId)
		}
	}
}
