package raft

import "time"

type AppendEntriesArgs struct {
	LeaderTerm         int
	LeaderId           int
	LeaderPrevLogIndex int // nextIndex[followerId]
	LeaderPrevLogTerm  int
	LeaderLogEntries   []byte
	LeaderCommitIndex  int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// makeAppendEntriesArgs
// uses nextIndex[server] - 1 for prev log index && term
// doesn't send over all logs just onest that need to be added to save space
func (rf *Raft) makeAppendEntriesArgs(server int) *AppendEntriesArgs {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	nextIndex := rf.nextIndex[server]

	args := &AppendEntriesArgs{
		LeaderTerm:        rf.currentTerm,
		LeaderId:          rf.me,
		LeaderCommitIndex: rf.commitIndex,
		// below only necessary for appending logs
		LeaderPrevLogIndex: nextIndex - 1,
		LeaderPrevLogTerm:  rf.logs[nextIndex-1].Term,
	}

	args.LeaderLogEntries = Compress(EncodeToBytes(make([]LogEntry, 0)))
	return args
}

// AppendEntries RPC
// called by follower invoked by current leader
// for replicating log entries + heartbeats
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	logBytes := Decompress(args.LeaderLogEntries)
	logs := DecodeToLogs(logBytes)

	if len(logs) != 0 {
		DPrint(rf.me, "AppendEntries RPC", "Called By %d", args.LeaderId)
	}

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

	// decrement nextIndex
	if args.LeaderPrevLogIndex > len(rf.logs)-1 {
		DPrint(rf.me, "AppendEntries RPC", "Unsuccessful; LeaderPrevLogIndex (%d) > len(rf.logs) -1 (%d); LeaderID: %d;", args.LeaderPrevLogIndex, len(rf.logs)-1, args.LeaderId)
		reply.Success = false
		return
	}

	// If leader's prevLogTerm != follower's prevLogTerm:
	// reply false and delete all existing entries from prevLogIndex forward
	if args.LeaderPrevLogIndex <= len(rf.logs)-1 &&
		args.LeaderPrevLogTerm != rf.logs[args.LeaderPrevLogIndex].Term {
		DPrint(rf.me, "AppendEntries RPC", "Unsuccessful; args.LeaderPrevLogTerm (%d) != rf.logs[args.LeaderPrevLogIndex].Term (%d); LeaderID: %d;", args.LeaderPrevLogIndex, args.LeaderPrevLogTerm, rf.logs[args.LeaderPrevLogIndex].Term)
		rf.logs = rf.logs[:args.LeaderPrevLogIndex]
		reply.Success = false
		return
	}

	// if heartbeat; update commitIndex and return
	if len(logs) != 0 {
		// send to consumer
		DPrint(rf.me, "AppendEntries RPC", "Called By %d; appending to logs", args.LeaderId)
		rf.logs = append(rf.logs, logs...)
		go rf.logQueueProducer(len(rf.logs) - 1)
	}
	// append new entries not already in the log

	// update commitIndex with highest known log entry
	if args.LeaderCommitIndex > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommitIndex, len(rf.logs)-1)
	}
}

// sendAppendEntry method
// single non-heartbeat AppendEntries RPC
// server (int) defines serverId RPC is for
func (rf *Raft) sendAppendEntry(server int, replicationChan chan int, isFollower chan bool) {
	DPrint(rf.me, "sendAppendEntry", "called for server %d", server)

	args := rf.makeAppendEntriesArgs(server)
	args.LeaderLogEntries = Compress(EncodeToBytes(rf.logs[args.LeaderPrevLogIndex+1:]))
	reply := &AppendEntriesReply{}

	for !rf.killed() {
		// DPrint(rf.me, "sendAppendEntry", "Calling Raft.AppendEntries RPC for %d; args: %v", server, args)
		ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
		// DPrint(rf.me, "sendAppendEntry", "Recevied Response Raft.AppendEntries RPC for %d; reply: %v", server, reply)

		if !ok {
			DPrint(rf.me, "sendAppendEntry", "RPC Error for %d", server)
			continue
		}

		if reply.Success {
			DPrint(rf.me, "sendAppendEntry", "RPC success for %d", server)
			rf.nextIndex[server]++
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

		rf.mu.Lock()
		rf.nextIndex[server]--
		nextIndex := rf.nextIndex[server]
		rf.mu.Unlock()

		args.LeaderPrevLogIndex = nextIndex - 1
		args.LeaderPrevLogTerm = rf.logs[nextIndex-1].Term
		args.LeaderLogEntries = Compress(EncodeToBytes(rf.logs[nextIndex:]))

		DPrint(rf.me, "sendAppendEntry", "Repeating request %d", server)
		time.Sleep(10 * time.Millisecond)
	}
}

// sendLogEntries
func (rf *Raft) sendLogEntries() {
	peerCount := len(rf.peers)
	replicationCount := 0
	replicationsNeeded := (peerCount / 2)
	replicationChan := make(chan int, peerCount-1)
	isFollower := make(chan bool, peerCount-1)

	DPrint(rf.me, "sendLogEntries", "called; replicationsNeeded: %d; peers: %v", replicationsNeeded, rf.peers)

	for serverId := range peerCount {
		if serverId != rf.me {
			go rf.sendAppendEntry(serverId, replicationChan, isFollower)
		}
	}

	// determine quorum of logs sent to finalize commit
	for range peerCount - 1 {
		select {
		case outcome := <-replicationChan:
			replicationCount += outcome
			DPrint(rf.me, "sendLogEntries", "replicaitonCount: %d; outcome: %d; replicationsNeeded: %d", replicationCount, outcome, replicationsNeeded)
			if replicationCount >= replicationsNeeded {
				DPrint(rf.me, "sendLogEntries", "replicationsNeeded >= replicaitonCount == %d >= %d;", replicationsNeeded, replicationCount)
				rf.mu.Lock()
				rf.commitIndex++
				rf.mu.Unlock()
				go rf.logQueueProducer(len(rf.logs) - 1)
				return
			}
		case <-isFollower:
			return
		}
	}
}

// sendHeartbeat method: sends single heartbeat
// server (int) defines serverId RPC is for
func (rf *Raft) sendHeartbeat(server int) {
	args := rf.makeAppendEntriesArgs(server)
	reply := &AppendEntriesReply{}

	// DPrint(rf.me, "sendHearbeat", "sending to %d", server)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	// DPrint(rf.me, "sendHearbeat", "recieved from %d", server)

	// convert to follower
	if ok && !reply.Success && reply.Term > args.LeaderTerm {
		rf.becomeFollower(reply.Term)
	}
}

// sendHeartbeats method
// triggered by leader sending empty AppendEntries RPCs to followers
func (rf *Raft) sendHeartbeats() {
	// DPrint(rf.me, "sendHearbeatS", "isLeader=%t", rf.getState() == Leader)
	for serverId := range len(rf.peers) {
		if serverId != rf.me {
			go rf.sendHeartbeat(serverId)
		}
	}
}
