package raft

import "time"

type RequestVoteArgs struct {
	CandidateTerm         int
	CandidateId           int
	CandidateLastLogIndex int
	CandidateLastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	voteGranted bool
}

// RequestVote RPC
// called by voter (current Raft instance) and is initiated by candidate requesting vote
// voter determines if it will vote for candidate and returns reply
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// if candidates term < voters term; candidate becomes follower
	if args.CandidateTerm < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.voteGranted = false
		return
	}

	isVoterValid := rf.votedFor == -1 || rf.votedFor == args.CandidateId
	// candidates log is at least as up to date as voters log
	isCandidateValid := args.CandidateLastLogIndex >= len(rf.logs)-1

	if isVoterValid && isCandidateValid {
		rf.currentTerm = reply.Term
		reply.voteGranted = true
	}

}

// leaders must check that the term hasn't changed since sending the RPC
// leaders must account for the possibility that replies from concurrent RPCs to the same follower have changed the leader's state (e.g. nextIndex).

// sendRequestVote method
// called by candidates during election to request a vote from a Raft instance
func (rf *Raft) sendRequestVote(server int, es *ElectionState, args *RequestVoteArgs) bool {
	reply := RequestVoteReply{}
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type ElectionState struct {
	voteCount         int
	convertToFollower bool
	newTerm           int
}

// startElection method
// called by follower if no communication received by leader
// over election timeout.
//
// Three outcomes:
// 1) Candidate wins: send heartbeats
// 2) Another server is leader: return to follower state
// 3) No win or lose: start over process
func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.currentTerm++
	rf.state = Candidate
	peerCount := len(rf.peers)
	instanceId := rf.me
	votesNeeded := (peerCount / 2) + 1

	es := ElectionState{voteCount: 1} // votes for self
	args := RequestVoteArgs{
		CandidateId:           instanceId,
		CandidateTerm:         rf.currentTerm,
		CandidateLastLogIndex: len(rf.logs) - 1,
		CandidateLastLogTerm:  rf.logs[len(rf.logs)-1].Term,
	}

	rf.mu.Unlock()

	// issues `RequestVote RPCs` in parallel
	for server := range peerCount {
		if server != instanceId {
			go rf.sendRequestVote(server, &es, &args)
		}
	}

	// Outcome 2: a follower is actually a leader
	if es.convertToFollower {
		rf.mu.Lock()
		rf.currentTerm = es.newTerm
		rf.state = Follower
		rf.lastHeardFromLeader = time.Now()
		rf.votedFor = -1
		rf.mu.Unlock()
	} else if es.voteCount >= votesNeeded {
		// Outcome 1: elected to leader
		rf.mu.Lock()
		rf.state = Leader
		rf.mu.Unlock()
		// TODO: send heartbeats
	}
	// Outcome 3: repeate election
}
