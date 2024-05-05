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

// all conflict arguements refer to follower
type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictTerm  int
	ConflictIndex int
	ConflictLen   int
	Recieved      bool
}

func (rf *Raft) applyLogs() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	rf.persist()
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		msg := ApplyMsg{
			CommandValid: true,
			Command:      rf.logs[i-1].Command,
			CommandIndex: i,
		}
		rf.applyCh <- msg
		rf.lastApplied = i
	}
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
		go rf.applyLogs()
	}
}

// sendAppendEntry method: leader only
// single non-heartbeat AppendEntries RPC
// server (int) defines serverId RPC is for
func (rf *Raft) sendAppendEntry(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	if rf.getState() != Leader || !ok || !reply.Recieved {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if reply.Success {
		rf.matchIndex[server] = args.LeaderPrevLogIndex + args.LeaderLogEntryLen
		rf.nextIndex[server] = rf.matchIndex[server] + 1

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

		go rf.applyLogs()
		return
	}

	if reply.Term > args.LeaderTerm {
		rf.votedFor = -1
		rf.state = Follower
		rf.currentTerm = reply.Term
		return
	}

	// Optimized handling for finding where leader and follower logs match;
	// better than decrementing next index by one each RPC
	//
	// Case 1:
	// 	- follower didnt have an entry at prevLogIndex (next index - 1)
	// 	- set next index to followers length
	if reply.ConflictIndex == -1 && reply.ConflictTerm == -1 {
		rf.nextIndex[server] = reply.ConflictLen
		return
	}

	// Case 2:
	// 	- follower has entry at prevLogIndex but term doesnt match with leaders
	//  - find the last index where the conflict term is in logs
	lastIndexOfConflictTerm := len(rf.logs)
	for lastIndexOfConflictTerm > 1 && rf.logs[lastIndexOfConflictTerm-1].Term != reply.ConflictTerm {
		lastIndexOfConflictTerm--
	}

	if lastIndexOfConflictTerm == 0 {
		// Case 2A:
		// 	- reply.ConflictTerm is NOT in logs
		// 	- set nextIndex to the index where ConflictTerm first appears in followers log
		rf.nextIndex[server] = reply.ConflictIndex
	} else {
		// Case 2B:
		// 	- reply.ConflictTerm is in logs
		// 	- set nextIndex to the last index where ConflictTerm appears in the leaders log
		rf.nextIndex[server] = lastIndexOfConflictTerm
	}
	time.Sleep(10 * time.Millisecond)
}

// sendLogEntries: leader method
// called by leader when client calls Start() and sends new log to all peers
// for replication.
func (rf *Raft) sendLogEntries() {
	if rf.getState() != Leader {
		return
	}

	rf.mu.Lock()
	for serverId := range rf.peerCount {
		if serverId != rf.me {

			args := &AppendEntriesArgs{
				LeaderTerm:         rf.currentTerm,
				LeaderId:           rf.me,
				LeaderCommitIndex:  rf.commitIndex,
				LeaderPrevLogIndex: max(rf.nextIndex[serverId]-1, 0),
				LeaderPrevLogTerm:  0,
			}

			// ignores situations where no previous logs; either empty or single log
			if args.LeaderPrevLogIndex > 0 {
				args.LeaderPrevLogTerm = rf.logs[args.LeaderPrevLogIndex-1].Term
			}

			arr := rf.logs[args.LeaderPrevLogIndex:]
			args.LeaderLogEntryLen = len(arr)
			args.LeaderLogEntries = Compress(EncodeToBytes(arr))

			go rf.sendAppendEntry(serverId, args, &AppendEntriesReply{})
		}
	}

	rf.mu.Unlock()
}
