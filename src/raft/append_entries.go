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
	leader := args.LeaderId

	// let leader know it is behind if their term < instances
	if args.LeaderTerm < rf.currentTerm {
		DPrint(rf.me, "AppendEntries RPC", "Unsuccessful; Leader behind follower; leader=%d", leader)
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
	if args.LeaderPrevLogIndex > len(rf.logs) {
		DPrint(rf.me, "AppendEntries RPC", "Unsuccessful; LeaderPrevLogIndex (%d) > LogIndex (%d); leader=%d", args.LeaderPrevLogIndex, len(rf.logs), leader)
		reply.Success = false
		return
	}

	// If leader's prevLogTerm != follower's prevLogTerm:
	// reply false and delete all existing entries from prevLogIndex forward
	// If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it (§5.3)
	if args.LeaderPrevLogIndex != 0 && args.LeaderPrevLogIndex <= len(rf.logs) &&
		args.LeaderPrevLogTerm != rf.logs[args.LeaderPrevLogIndex-1].Term {
		DPrint(rf.me, "AppendEntries RPC", "Unsuccessful; LeaderPrevLogTerm (%d) != rf.logs[%d - 1].Term (%d); currentTerm=%d leader=%d", args.LeaderPrevLogTerm, args.LeaderPrevLogIndex, rf.logs[args.LeaderPrevLogIndex-1].Term, rf.currentTerm, leader)
		rf.logs = rf.logs[:args.LeaderPrevLogIndex-1]
		reply.Success = false
		return
	}

	// append new entries not already in the log
	// send to consumer
	if len(logs) != 0 {
		rf.logs = append(rf.logs[:args.LeaderPrevLogIndex], logs...)
		DPrint(rf.me, "AppendEntries RPC", "Success info; LeaderPrevLogTerm=%d; LeaderPrevLogIndex=%d; currentIndex=%d; leader=%d", args.LeaderPrevLogTerm, args.LeaderPrevLogIndex, len(rf.logs), leader)
	}

	// update commitIndex with highest known log entry
	if args.LeaderCommitIndex > rf.commitIndex {
		lastNewEntryIndex := args.LeaderPrevLogIndex + len(logs)
		rf.commitIndex = min(args.LeaderCommitIndex, lastNewEntryIndex)
		DPrint(rf.me, "AppendEntries RPC", "Updated commitIndex=%d; leader=%d", rf.commitIndex, leader)
	}

	if rf.commitIndex > rf.lastApplied {
		go rf.logQueueProducer(rf.commitIndex)
	}
}

// sendAppendEntry method: leader only
// single non-heartbeat AppendEntries RPC
// server (int) defines serverId RPC is for
// TestFailAgree2B
func (rf *Raft) sendAppendEntry(server int, replicationChan chan int, isFollower chan bool) {

	for !rf.killed() && rf.getState() == Leader {
		// need to make new params for RPC
		// or else error occurs testing
		reply := &AppendEntriesReply{}

		args := rf.makeAppendEntriesArgs(server)
		arr := rf.logs[args.LeaderPrevLogIndex:]
		args.LeaderLogEntries = Compress(EncodeToBytes(arr))

		DPrint(rf.me, "sendAppendEntry", "called for server %d; args.LeaderPrevLogIndex: %d; appending log: %v; logs: %v", server, args.LeaderPrevLogIndex, arr, rf.logs)

		ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

		DPrint(rf.me, "sendAppendEntry", "Recevied Response Raft.AppendEntries RPC for %d; reply: %v", server, reply)

		// if there is an error retry the reqeuest
		if !ok {
			continue
		}

		// if success, update servers matchIndex and nextIndex
		// use prevLogIndex + # logs added if state has changed
		// then pass vote to replication channel
		if reply.Success {
			rf.mu.Lock()
			DPrint(rf.me, "sendAppendEntry", "Updating server=%d match index=%d", server, args.LeaderPrevLogIndex+len(arr))
			rf.matchIndex[server] = args.LeaderPrevLogIndex + len(arr)
			rf.nextIndex[server] = args.LeaderPrevLogIndex + len(arr) + 1
			rf.mu.Unlock()
			rf.calculateCommitIndex()
			go rf.logQueueProducer(rf.commitIndex)
			replicationChan <- 1
			return
		}

		// the follower has a higher term
		// convert leader to follower
		// pass update to isFollower channel
		if reply.Term > args.LeaderTerm {
			rf.becomeFollower(reply.Term)
			isFollower <- true
			return
		}

		// unsuccessful so nextIndex is too high;
		// must decrement
		rf.mu.Lock()
		rf.nextIndex[server]--
		rf.mu.Unlock()

		time.Sleep(10 * time.Millisecond)
	}
}

// sendLogEntries: leader method
// called by leader when client calls Start() and sends new log to all peers
// for replication. successful if a majority of peers replicate the log
// in-memory. the leader then updates it's commitIndex and processes logs
// up to that new commitIndex. if leader is a follower it will be updated.
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

	for range peerCount - 1 {
		select {
		// determine quorum of logs sent to finalize commit
		case outcome := <-replicationChan:
			replicationCount += outcome
			if replicationCount >= replicationsNeeded {
				rf.calculateCommitIndex()
				go rf.logQueueProducer(rf.commitIndex)
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
