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
	DPrintf("raft %d; RequestVote; received initialization from %d", rf.me, args.CandidateId)

	// if candidates term < voters term; candidate becomes follower
	if args.CandidateTerm < rf.currentTerm {
		DPrintf("raft %d; RequestVote; candidate %d should step down", rf.me, args.CandidateId)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	isVoterValid := rf.votedFor == -1 || rf.votedFor == args.CandidateId
	// candidates log is at least as up to date as voters log
	isCandidateValid := args.CandidateLastLogIndex >= len(rf.logs)

	if isVoterValid && isCandidateValid {
		DPrintf("raft %d; RequestVote; voted for candidate %d", rf.me, args.CandidateId)
		rf.currentTerm = reply.Term
		reply.VoteGranted = true
	}
}

// leaders must check that the term hasn't changed since sending the RPC
// leaders must account for the possibility that replies from concurrent RPCs to the same follower have changed the leader's state (e.g. nextIndex).

// sendRequestVote method
// called by candidates during election to request a vote from a Raft instance
func (rf *Raft) sendRequestVote(server int, voteChannel chan int, isFollowerChannel chan bool) {
	args := rf.makeRequestVoteArgs()
	reply := &RequestVoteReply{}

	DPrintf("raft %d; sendRequestVote; sending to %d", rf.me, server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	DPrintf("raft %d; sendRequestVote; received reply from %d", rf.me, server)

	if !ok {
		//  !ok means that there was an error and should re-send the request vote
		voteChannel <- 0
	} else {
		if reply.Term > args.CandidateTerm {
			//  Another server is leader: return to follower state
			rf.mu.Lock()
			rf.lastHeardFromLeader = time.Now()
			rf.votedFor = -1
			rf.state = Follower
			rf.currentTerm = reply.Term
			rf.mu.Unlock()
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
	DPrintf("raft %d: called startElection", rf.me)
	rf.currentTerm++
	rf.state = Candidate
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
	DPrint(rf.me, "becomeLeader", "is now leader")
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
}
