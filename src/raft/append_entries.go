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
	DPrint(rf.me, "AppendEntries RPC", "Called By %d; Heartbeat: %t; LeaderTerm: %d; CurrentTerm: %d", args.LeaderId, len(args.LeaderLogEntries) == 0, args.LeaderTerm, rf.currentTerm)

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

	DPrint(rf.me, "AppendEntries RPC", "Logs appended for %d; Heartbeat: false", args.LeaderId)
}

// apply()
// applies command to state machine
func (rf *Raft) apply() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := len(rf.logs) - 1
	msg := ApplyMsg{
		CommandValid: true,
		Command:      rf.logs[index].Command,
		CommandIndex: index,
	}

	if msg.CommandIndex <= rf.lastApplied {
		return
	}

	rf.lastApplied = index

	rf.applyCh <- msg
}

// sendAppendEntry method
// single non-heartbeat AppendEntries RPC
// server (int) defines serverId RPC is for
func (rf *Raft) sendAppendEntry(server int, replicationChan chan int, isFollower chan bool) {
	DPrint(rf.me, "sendAppendEntry", "called for server %d", server)

	for !rf.killed() {
		args := rf.makeAppendEntriesArgs(server)
		reply := &AppendEntriesReply{}

		DPrint(rf.me, "sendAppendEntry", "Initiating RPC for %d", server)
		ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
		DPrint(rf.me, "sendAppendEntry", "Completed RPC for %d", server)

		if !ok {
			DPrint(rf.me, "sendAppendEntry", "RPC Error for %d", server)
			continue
		}

		if reply.Success {
			DPrint(rf.me, "sendAppendEntry", "RPC success for %d", server)
			replicationChan <- 1
			return
		}

		// convert to follower; dont retry?
		if reply.Term > args.LeaderTerm {
			DPrint(rf.me, "sendAppendEntry", "Converting to follower")
			rf.becomeFollower(reply.Term)
			isFollower <- true
			return
		}

		rf.nextIndex[server]--
		DPrint(rf.me, "sendAppendEntry", "Repeating request %d", server)
		time.Sleep(10 * time.Millisecond)
	}
}

// sendLogEntries
func (rf *Raft) sendLogEntries() {
	DPrint(rf.me, "sendLogEntries", "called")
	var msg ApplyMsg
	peerCount := len(rf.peers)
	replicationCount := 0
	replicationsNeeded := (peerCount / 2)
	replicationChan := make(chan int, peerCount-1)
	isFollower := make(chan bool, peerCount-1)

	for serverId := range peerCount {
		if serverId != rf.me {
			go rf.sendAppendEntry(serverId, replicationChan, isFollower)
		}
	}

	// determine quorum of logs sent to finalize commit
OuterLoop:
	for range peerCount - 1 {
		select {
		case outcome := <-replicationChan:
			replicationCount += outcome
			if replicationsNeeded == replicationCount {
				// TODO: not sure if correct; needs testing
				rf.mu.Lock()
				rf.commitIndex = max(rf.commitIndex, len(rf.logs))
				msg = ApplyMsg{
					CommandValid: true,
					Command:      rf.logs[len(rf.logs)-1].Command,
					CommandIndex: len(rf.logs) - 1,
				}
				rf.mu.Unlock()
				break OuterLoop
			}
		case <-isFollower:
			break OuterLoop
		}
	}

	rf.applyCh <- msg
}

// sendHeartbeat method: sends single heartbeat
// server (int) defines serverId RPC is for
func (rf *Raft) sendHeartbeat(server int) {
	args := rf.makeAppendEntriesArgs(server)
	args.LeaderLogEntries = make([]LogEntry, 0)
	reply := &AppendEntriesReply{}

	DPrint(rf.me, "sendHearbeat", "sending to %d", server)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	DPrint(rf.me, "sendHearbeat", "recieved from %d", server)

	// convert to follower
	if ok && !reply.Success && reply.Term > args.LeaderTerm {
		DPrint(rf.me, "sendHearbeat", "converted to follower")
		rf.becomeFollower(reply.Term)
	}
}

// sendHeartbeats method
// triggered by leader sending empty AppendEntries RPCs to followers
func (rf *Raft) sendHeartbeats() {
	DPrint(rf.me, "sendHearbeatS", "isLeader=%t", rf.getState() == Leader)
	for serverId := range len(rf.peers) {
		if serverId != rf.me {
			go rf.sendHeartbeat(serverId)
		}
	}
}
