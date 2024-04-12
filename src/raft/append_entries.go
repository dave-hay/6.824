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
	args := &AppendEntriesArgs{
		LeaderTerm:        rf.currentTerm,
		LeaderId:          rf.me,
		LeaderCommitIndex: rf.commitIndex,
		// below only necessary for appending logs
		LeaderPrevLogIndex: 0,
		LeaderPrevLogTerm:  0,
	}

	serverPrevLogIndex := rf.nextIndex[server] - 1

	if serverPrevLogIndex != 0 {
		args.LeaderPrevLogIndex = serverPrevLogIndex
		args.LeaderPrevLogTerm = rf.logs[serverPrevLogIndex-1].Term
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
	uid := generateUID()

	if len(logs) != 0 {
		DPrint(rf.me, "AppendEntries RPC", "Called By %d to append logs: %v; uid: %s", args.LeaderId, logs, uid)
	}

	// let leader know it is behind if their term < instances
	if args.LeaderTerm < rf.currentTerm {
		DPrint(rf.me, "AppendEntries RPC", "Unsuccessful; Leader behind follower %s", uid)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// reset follower so leader keeps authority
	rf.lastHeardFromLeader = time.Now()
	rf.votedFor = -1
	rf.state = Follower
	rf.currentTerm = args.LeaderTerm
	reply.Term = args.LeaderTerm
	reply.Success = true

	// decrement nextIndex
	if len(rf.logs) != 0 && args.LeaderPrevLogIndex > len(rf.logs) {
		DPrint(rf.me, "AppendEntries RPC", "Unsuccessful; LeaderPrevLogIndex (%d) > LogIndex (%d); uid: %s", args.LeaderPrevLogIndex, len(rf.logs), uid)
		reply.Success = false
		return
	}

	// If leader's prevLogTerm != follower's prevLogTerm:
	// reply false and delete all existing entries from prevLogIndex forward
	if args.LeaderPrevLogIndex != 0 && args.LeaderPrevLogIndex <= len(rf.logs) &&
		args.LeaderPrevLogTerm != rf.logs[args.LeaderPrevLogIndex-1].Term {
		DPrint(rf.me, "AppendEntries RPC", "Unsuccessful; LeaderPrevLogTerm (%d) != rf.logs[%d - 1].Term (%d); uid %s", args.LeaderPrevLogTerm, args.LeaderPrevLogIndex, rf.logs[args.LeaderPrevLogIndex-1].Term, uid)
		rf.logs = rf.logs[:args.LeaderPrevLogIndex-1]
		reply.Success = false
		return
	}

	// append new entries not already in the log
	// send to consumer
	if len(logs) != 0 {
		rf.logs = append(rf.logs, logs...)
		DPrint(rf.me, "AppendEntries RPC", "Success; Appended logs: %v; uid: %s", rf.logs, uid)
		go rf.logQueueProducer(len(rf.logs))
	}

	// update commitIndex with highest known log entry
	if args.LeaderCommitIndex > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommitIndex, len(rf.logs))
	}
}

// sendAppendEntry method
// single non-heartbeat AppendEntries RPC
// server (int) defines serverId RPC is for
// TestFailAgree2B
func (rf *Raft) sendAppendEntry(server int, replicationChan chan int, isFollower chan bool) {

	for !rf.killed() {
		reply := &AppendEntriesReply{}

		args := rf.makeAppendEntriesArgs(server)
		arr := rf.logs[args.LeaderPrevLogIndex:]
		args.LeaderLogEntries = Compress(EncodeToBytes(arr))
		DPrint(rf.me, "sendAppendEntry", "called for server %d; args.LeaderPrevLogIndex: %d; appending log: %v; logs: %v", server, args.LeaderPrevLogIndex, arr, rf.logs)

		ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
		DPrint(rf.me, "sendAppendEntry", "Recevied Response Raft.AppendEntries RPC for %d; reply: %v", server, reply)

		if !ok {
			DPrint(rf.me, "sendAppendEntry", "RPC Error for %d", server)
			continue
		}

		if reply.Success {
			DPrint(rf.me, "sendAppendEntry", "RPC success for %d", server)
			// Instead, the correct thing to do is update matchIndex to be prevLogIndex + len(entries[]) from the arguments you sent in the RPC originally.
			rf.mu.Lock()
			rf.matchIndex[server] = args.LeaderPrevLogIndex + len(arr)
			rf.nextIndex[server] = args.LeaderPrevLogIndex + len(arr) + 1
			rf.mu.Unlock()
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
		rf.mu.Unlock()

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
				go rf.logQueueProducer(len(rf.logs))
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
	if ok && !reply.Success {
		if reply.Term > args.LeaderTerm {
			rf.becomeFollower(reply.Term)
		} else {
			rf.mu.Lock()
			rf.nextIndex[server]--
			rf.mu.Unlock()
		}
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
