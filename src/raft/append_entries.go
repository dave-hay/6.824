package raft

import (
	"slices"
	"time"
)

type AppendEntriesArgs struct {
	LeaderTerm         int
	LeaderId           int
	LeaderPrevLogIndex int // nextIndex[followerId]
	LeaderPrevLogTerm  int
	LeaderLogEntries   []byte
	LeaderLogEntryLen  int
	LeaderCommitIndex  int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictTerm  int
	ConflictIndex int
	ConflictLen   int
	Recieved      bool
}

// makeAppendEntriesArgs
// uses nextIndex[server] - 1 for prev log index && term
// doesn't send over all logs just onest that need to be added to save space
func (rf *Raft) makeAppendEntriesArgs(server int) *AppendEntriesArgs {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	args := &AppendEntriesArgs{
		LeaderTerm:         rf.currentTerm,
		LeaderId:           rf.me,
		LeaderCommitIndex:  rf.commitIndex,
		LeaderPrevLogIndex: 0,
		LeaderPrevLogTerm:  0,
	}

	serverNextLogIndex := rf.nextIndex[server]

	// if logIndex=1 then no previous log entries
	// if logIndex=0 (no logs) then not applicable
	if serverNextLogIndex > 1 {
		args.LeaderPrevLogIndex = serverNextLogIndex - 1
		args.LeaderPrevLogTerm = rf.logs[args.LeaderPrevLogIndex-1].Term
	}

	arr := rf.logs[args.LeaderPrevLogIndex:]
	args.LeaderLogEntryLen = len(arr)
	args.LeaderLogEntries = Compress(EncodeToBytes(arr))

	return args
}

// AppendEntries RPC
// called by follower invoked by current leader
// for replicating log entries + heartbeats
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Recieved = true
	logBytes := Decompress(args.LeaderLogEntries)
	logs := DecodeToLogs(logBytes)

	// let leader know it is behind if their term < instances
	if args.LeaderTerm < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	defer rf.persist()
	// reset follower so leader keeps authority
	rf.lastHeardFromLeader = time.Now()
	rf.votedFor = -1
	rf.state = Follower
	rf.currentTerm = args.LeaderTerm
	reply.Term = args.LeaderTerm
	reply.Success = true

	// decrement nextIndex
	if args.LeaderPrevLogIndex > len(rf.logs) {
		reply.Success = false
		reply.ConflictTerm = -1
		reply.ConflictIndex = -1
		// bypass the iterative approach to conflicts
		// next prevLogIndex will be len(rf.logs)
		reply.ConflictLen = len(rf.logs)
		return
	}

	// If leader's prevLogTerm != follower's prevLogTerm:
	// reply false and delete all existing entries from prevLogIndex forward
	// If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it (§5.3)
	if args.LeaderPrevLogIndex != 0 && len(rf.logs) != 0 && args.LeaderPrevLogTerm != rf.logs[args.LeaderPrevLogIndex-1].Term {
		reply.ConflictTerm = rf.logs[args.LeaderPrevLogIndex-1].Term

		// find left most index of ConflictTerm
		curIndex := 1
		for rf.logs[curIndex-1].Term != reply.ConflictTerm {
			curIndex++
		}

		rf.logs = rf.logs[:curIndex]
		reply.ConflictIndex = curIndex
		reply.Success = false
		return
	}

	// append new entries not already in the log
	if len(logs) != 0 {
		rf.logs = append(rf.logs[:args.LeaderPrevLogIndex], logs...)
	}

	// update commitIndex with highest known log entry
	if args.LeaderCommitIndex > rf.commitIndex {
		lastNewEntryIndex := args.LeaderPrevLogIndex + len(logs)
		rf.commitIndex = min(args.LeaderCommitIndex, lastNewEntryIndex)
	}

	if rf.commitIndex > rf.lastApplied {
		// go rf.applyLogs()
		go rf.logQueueProducer(rf.commitIndex)
	}
}

// sendAppendEntry method: leader only
// single non-heartbeat AppendEntries RPC
// server (int) defines serverId RPC is for
// TestFailAgree2B
func (rf *Raft) sendAppendEntry(server int, replicationChan chan int, isFollower chan bool) {
	reply := &AppendEntriesReply{}
	args := rf.makeAppendEntriesArgs(server)

	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	if rf.getState() != Leader {
		return
	}

	// if there is an error retry the reqeuest
	if !ok || !reply.Recieved {
		return
	}

	// if success, update servers matchIndex and nextIndex
	// use prevLogIndex + # logs added if state has changed
	// then pass vote to replication channel
	if reply.Success {
		rf.updateFollowerState(server, args.LeaderPrevLogIndex, args.LeaderLogEntryLen)
		replicationChan <- 1
		return
	}

	// the follower has a higher term
	// convert leader to follower
	// pass update to isFollower channel
	if reply.Term > args.LeaderTerm {
		rf.becomeFollower(reply.Term, false)
		isFollower <- true
		return
	}

	rf.findNextIndex(server, reply.ConflictIndex, reply.ConflictTerm, reply.ConflictLen)
	time.Sleep(10 * time.Millisecond)
}

func (rf *Raft) updateFollowerState(server int, prevLogIndex int, logEntryLen int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.matchIndex[server] = prevLogIndex + logEntryLen
	rf.nextIndex[server] = prevLogIndex + logEntryLen + 1

	// once a log has been replicated on a majority of nodes it is considered
	// committed and the leaders commitIndex can be updated based on the
	// following rules:
	//   - a majority of matchIndex[i]'s ≥ N: by picking the middle index in a
	//     sorted list it implies all to the left are >= that value.
	//   - newIndex > commitIndex: can't go backwards
	//   - log[newIndex].term == currentTerm: can't go backwards
	s := make([]int, 0, rf.peerCount)
	s = append(s, len(rf.logs))

	for i := range rf.peerCount {
		if i != rf.me {
			s = append(s, rf.matchIndex[i])
		}
	}

	slices.Sort(s)
	index := s[rf.peerCount/2]

	if index != -1 && index > rf.commitIndex && rf.logs[index-1].Term == rf.currentTerm {
		rf.commitIndex = index
	}

	// go rf.applyLogs()
	go rf.logQueueProducer(rf.commitIndex)
}

// findNextIndex method: called by leader only;
// Optimized handling for finding where leader and follower logs match;
// all conflict arguements refer to follower
// conflictIndex: index of conflict; conflictTerm int: term of conflictIndex; conflictLen: length of followers logs;
func (rf *Raft) findNextIndex(server int, cIndex int, cTerm int, cLen int) {
	if rf.getState() != Leader {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Case 1: follower does not have an entry at args.prevLogIndex
	// reply.ConflictLength is set to the followers last entry
	if cIndex == -1 && cTerm == -1 {
		rf.nextIndex[server] = cLen
		return
	}

	// Case 2: followers log contains entry at args.prevLogIndex
	// but conflict on the term.
	//
	// we need to now check if reply.ConflictTerm is in logs and
	// if it is we need the last (right most) index of an entry with Term=reply.ConflictTerm
	lastIndexOfConflictTerm := len(rf.logs)
	for lastIndexOfConflictTerm > 1 && rf.logs[lastIndexOfConflictTerm-1].Term != cTerm {
		lastIndexOfConflictTerm--
	}

	if lastIndexOfConflictTerm == 0 {
		// Case 2A: reply.ConflictTerm is NOT in logs
		// set nextIndex to the index where ConflictTerm first
		// appears in followers log
		rf.nextIndex[server] = cIndex
	} else {
		// Case 2B: reply.ConflictTerm is in logs
		// set nextIndex to the last index where ConflictTerm appears
		// in the leaders log
		rf.nextIndex[server] = lastIndexOfConflictTerm
	}
}

// sendLogEntries: leader method
// called by leader when client calls Start() and sends new log to all peers
// for replication. successful if a majority of peers replicate the log
// in-memory. the leader then updates it's commitIndex and processes logs
// up to that new commitIndex. if leader is a follower it will be updated.
func (rf *Raft) sendLogEntries() {
	if rf.getState() != Leader {
		return
	}
	// numGoroutines := runtime.NumGoroutine()
	// fmt.Printf("Number of Running Goroutines: %d\n", numGoroutines)
	replicationCount := 1
	replicationChan := make(chan int, rf.peerCount-1)
	isFollower := make(chan bool, rf.peerCount-1)

	for serverId := range rf.peerCount {
		if serverId != rf.me {
			go rf.sendAppendEntry(serverId, replicationChan, isFollower)
		}
	}

	for range rf.peerCount - 1 {
		select {
		// determine quorum of logs sent to finalize commit
		case outcome := <-replicationChan:
			replicationCount += outcome
			if replicationCount >= (rf.peerCount/2)+1 {
				// rf.calculateCommitIndex()
				// go rf.logQueueProducer(rf.commitIndex)
				return
			}
		case <-isFollower:
			return
		}
	}
}
