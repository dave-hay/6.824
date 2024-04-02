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
func (rf *Raft) sendRequestVote(server int, voteChannel chan int, errChannel chan int) {

	rf.mu.Lock()
	args := &RequestVoteArgs{
		CandidateId:           rf.me,
		CandidateTerm:         rf.currentTerm,
		CandidateLastLogIndex: len(rf.logs) - 1,
		CandidateLastLogTerm:  rf.logs[len(rf.logs)-1].Term,
	}
	reply := &RequestVoteReply{}
	rf.mu.Unlock()

	// keep calling while error
	// keep calling until timeout
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	if !ok {
		//  !ok means that there was an error and should re-send the request vote
		errChannel <- server
		return
	}

	if reply.voteGranted {
		voteChannel <- 1
	} else if reply.Term > args.CandidateTerm {
		//  Another server is leader: return to follower state
		rf.mu.Lock()
		rf.lastHeardFromLeader = time.Now()
		rf.votedFor = -1
		rf.state = Follower
		rf.currentTerm = reply.Term
		rf.mu.Unlock()
	}
	voteChannel <- 0
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
	voteCount := 0
	rf.lastHeardFromLeader = time.Now()

	voteChannel := make(chan int, peerCount-1)
	errChannel := make(chan int, peerCount-1)
	rf.mu.Unlock()

	// issues `RequestVote RPCs` in parallel
	for server := range peerCount {
		if server != instanceId {
			go rf.sendRequestVote(server, voteChannel, errChannel)
		}
	}

	for range peerCount - 1 {
		select {
		case vote := <-voteChannel:
			voteCount += vote
		}
	}

	// Outcome 1: elected to leader
	if voteCount >= votesNeeded {
		rf.mu.Lock()
		rf.state = Leader
		rf.mu.Unlock()
		rf.sendHeartbeats()
	}
	// Outcome 3: repeat election
}
