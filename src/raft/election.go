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

func (rf *Raft) makeRequestVoteArgs() *RequestVoteArgs {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	args := &RequestVoteArgs{
		CandidateId:           rf.me,
		CandidateTerm:         rf.currentTerm,
		CandidateLastLogIndex: 0,
		CandidateLastLogTerm:  0,
	}

	if len(rf.logs) != 0 {
		args.CandidateLastLogIndex = len(rf.logs)
		args.CandidateLastLogTerm = rf.logs[len(rf.logs)-1].Term
	}

	return args
}

// RequestVote RPC
// called by voter (current Raft instance) and is initiated by candidate requesting vote
// voter determines if it will vote for candidate and returns reply
// voter denies its vote if its own log is more up-to-date than that of the candidate.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	uid := generateUID()

	DPrint(rf.me, "RequestVote RPC", "Raft: %d requested vote; uid: %s", args.CandidateId, uid)

	// if candidates term < voters term; candidate becomes follower
	if args.CandidateTerm < rf.currentTerm {
		DPrint(rf.me, "RequestVote RPC", "Voter has larger term; uid: %s", uid)
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
		// TODO: Persist
		rf.state = Follower
	}

	if rf.commitIndex > rf.lastApplied {
		DPrint(rf.me, "RequestVote RPC", "commitIndex (%d) > lastApplied (%d)", rf.commitIndex, rf.lastApplied)
		go rf.logQueueProducer(rf.commitIndex)
	}

	// voter is valid if it has not voted or has already voted for candidate
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {

		// if log empty then automatically vote for candidate
		if len(rf.logs) == 0 {
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			rf.lastHeardFromLeader = time.Now()
			return
		}

		lastLogTerm := rf.logs[len(rf.logs)-1].Term

		// if logs have last entries with different terms, log w/later term wins (5.4.1)
		if args.CandidateLastLogTerm > lastLogTerm {
			DPrint(rf.me, "RequestVote RPC", "Successful; CandidateLastLogTerm=%d > CurLastLogTerm=%d; uid: %s", args.CandidateLastLogTerm, lastLogTerm, uid)
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			rf.lastHeardFromLeader = time.Now()
			return
		}

		// if logs end with same term, longest log wins (5.4.1)
		if args.CandidateLastLogTerm == lastLogTerm && args.CandidateLastLogIndex >= len(rf.logs) {
			DPrint(rf.me, "RequestVote RPC", "Success; Same LogTerm + CandidateLastLogIndex=%d >= CurLastIndex=%d; uid: %s", args.CandidateLastLogIndex, len(rf.logs), uid)
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			rf.lastHeardFromLeader = time.Now()
			return
		}
	}

	reply.VoteGranted = false
}

// leaders must check that the term hasn't changed since sending the RPC
// leaders must account for the possibility that replies from concurrent RPCs to the same follower have changed the leader's state (e.g. nextIndex).

// sendRequestVote method
// called by candidates during election to request a vote from a Raft instance
func (rf *Raft) sendRequestVote(server int, voteChannel chan int, isFollowerChannel chan bool) {
	args := rf.makeRequestVoteArgs()
	reply := &RequestVoteReply{}

	// DPrintf("raft %d; sendRequestVote; sending to %d", rf.me, server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	// DPrintf("raft %d; sendRequestVote; received reply from %d; votegranted=%t; replyTerm=%d", rf.me, server, reply.VoteGranted, reply.Term)

	if !ok {
		//  !ok means that there was an error and should re-send the request vote
		voteChannel <- 0
	} else {
		if reply.Term > args.CandidateTerm {
			//  Another server is leader: return to follower state
			rf.becomeFollower(reply.Term)
			isFollowerChannel <- true
			return
		} else if reply.VoteGranted {
			voteChannel <- 1
		} else {
			voteChannel <- 0
		}
	}

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
	DPrint(rf.me, "startElection", "called")
	rf.currentTerm++
	rf.state = Candidate
	// TODO: Persist
	timeout := rf.getElectionTimeout()
	rf.mu.Unlock()

	peerCount := len(rf.peers)
	instanceId := rf.me
	votesNeeded := (peerCount / 2) + 1
	voteCount := 1

	voteChannel := make(chan int, peerCount-1)
	isFollowerChannel := make(chan bool, peerCount-1)

	// issues `RequestVote RPCs` in parallel
	for server := range peerCount {
		if server != instanceId {
			go rf.sendRequestVote(server, voteChannel, isFollowerChannel)
		}
	}

	for range peerCount - 1 {
		select {
		case vote := <-voteChannel:
			voteCount += vote
			// Outcome 1: elected to leader
			if voteCount >= votesNeeded {
				rf.becomeLeader()
				go rf.sendHeartbeats()
				return
			}
		case <-isFollowerChannel:
			voteCount = 0
			return
		case <-time.After(timeout):
			DPrint(rf.me, "startElection", "election timed out")
			return
		}
	}

	// if voteCount >= votesNeeded {
	// 	rf.becomeLeader()
	// 	go rf.sendHeartbeats()
	// 	return
	// }
	// Outcome 3: repeat election
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
	rf.mu.Unlock()

	peerCount := len(rf.peers)
	newNextIndex := make([]int, peerCount)
	newMatchIndex := make([]int, peerCount)

	for i := range peerCount {
		newNextIndex[i] = val
		newMatchIndex[i] = -1
	}

	rf.mu.Lock()
	rf.nextIndex = newNextIndex
	rf.matchIndex = newMatchIndex
	rf.mu.Unlock()
	// TODO: Persist
}

// becomeFollower() method
// currentTerm int: the most current term
func (rf *Raft) becomeFollower(currentTerm int) {
	DPrint(rf.me, "becomeFollower", "is now follower")
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.lastHeardFromLeader = time.Now()
	rf.votedFor = -1
	rf.state = Follower
	rf.currentTerm = currentTerm
	// TODO: Persist
}
