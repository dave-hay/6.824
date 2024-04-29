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
	VoteGranted bool
}

// RequestVote RPC
// called by voter (current Raft instance) and is initiated by candidate requesting vote
// voter determines if it will vote for candidate and returns reply
// voter denies its vote if its own log is more up-to-date than that of the candidate.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	// if candidates term < voters term; candidate becomes follower
	if args.CandidateTerm < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// if leaders term at least as large as candidate's term, then the
	// candidate returns to follower state. (5.2)
	// it's implied that the candidate's term >= current term
	if args.CandidateTerm > rf.currentTerm {
		rf.votedFor = -1
		rf.currentTerm = args.CandidateTerm
		rf.state = Follower
	}

	// voter is valid if it has not voted or has already voted for candidate
	// if log empty then automatically vote for candidate
	// if logs have last entries with different terms, log w/later term wins (5.4.1)
	// if logs end with same term, longest log wins (5.4.1)
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		if len(rf.logs) == 0 ||
			args.CandidateLastLogTerm > rf.logs[len(rf.logs)-1].Term ||
			args.CandidateLastLogTerm == rf.logs[len(rf.logs)-1].Term && args.CandidateLastLogIndex >= len(rf.logs) {
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			rf.lastHeardFromLeader = time.Now()
			return
		}
	}
}

// leaders must check that the term hasn't changed since sending the RPC
// leaders must account for the possibility that replies from concurrent RPCs to the same follower have changed the leader's state (e.g. nextIndex).

// sendRequestVote method
// called by candidates during election to request a vote from a Raft instance
func (rf *Raft) sendRequestVote(server int, voteChannel chan int, isFollowerChannel chan bool, args *RequestVoteArgs) {
	reply := &RequestVoteReply{}

	if ok := rf.peers[server].Call("Raft.RequestVote", args, reply); !ok {
		voteChannel <- 0
		return
	}

	if reply.VoteGranted {
		voteChannel <- 1
		return
	}

	rf.mu.Lock()
	sameTerm := args.CandidateTerm == rf.currentTerm
	rf.mu.Unlock()

	if reply.Term > args.CandidateTerm || !sameTerm {
		//  Another server is leader: return to follower state
		rf.becomeFollower(reply.Term, false)
		isFollowerChannel <- true
		return
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
	rf.becomeCandidate()

	args := &RequestVoteArgs{
		CandidateId:           rf.me,
		CandidateTerm:         rf.currentTerm,
		CandidateLastLogIndex: len(rf.logs),
		CandidateLastLogTerm:  0,
	}

	if len(rf.logs) != 0 {
		args.CandidateLastLogTerm = rf.logs[len(rf.logs)-1].Term
	}

	rf.persist()
	rf.mu.Unlock()

	peerCount := len(rf.peers)
	voteCount := 1
	voteChannel := make(chan int, peerCount-1)
	isFollowerChannel := make(chan bool, peerCount-1)

	// issues `RequestVote RPCs` in parallel
	for server := range peerCount {
		if server != rf.me {
			go rf.sendRequestVote(server, voteChannel, isFollowerChannel, args)
		}
	}

	for range peerCount - 1 {
		select {
		case vote := <-voteChannel:
			voteCount += vote
			// Outcome 1: elected to leader
			if voteCount >= (peerCount/2)+1 {
				rf.becomeLeader()
				return
			}
		case <-isFollowerChannel:
			voteCount = 0
			return
		case <-time.After(rf.getElectionTimeout()):
			// Outcome 3: repeat election
			DPrint(rf.me, "startElection", "election timed out")
			return
		}
	}
}

func (rf *Raft) becomeCandidate() {
	rf.currentTerm++
	rf.state = Candidate
	rf.lastHeardFromLeader = time.Now()
}

// becomeLeader() method
// updates state to reflect Leader
// initializes the new nextIndex[] array
// state is locked until completed
func (rf *Raft) becomeLeader() {
	DPrint(rf.me, "becomeLeader", "called")
	rf.mu.Lock()
	rf.state = Leader
	val := len(rf.logs) + 1 // last log index + 1

	for i := range rf.peerCount {
		rf.nextIndex[i] = val
		rf.matchIndex[i] = -1
	}

	rf.persist()
	rf.mu.Unlock()

	go rf.sendLogEntries()
}

// becomeFollower() method
// updates rafts state to follower and sets new currentTerm
// currentTerm int: the most current term
// restartTimer bool: optionally restart time
func (rf *Raft) becomeFollower(currentTerm int, restartTimer bool) {
	DPrint(rf.me, "becomeFollower", "is now follower")
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	rf.votedFor = -1
	rf.state = Follower
	rf.currentTerm = currentTerm
	if restartTimer {
		rf.lastHeardFromLeader = time.Now()
	}
}
