package raft

type RequestVoteArgs struct {
	CandidateTerm int
	CandidateId   int
	LastLogIndex  int // index of candidates last log entry
	LastLogTerm   int // term of candidates last log entry
}

type RequestVoteReply struct {
	CurrentTerm int  // for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// RPC invoked by candidates to gather votes
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// reply false if term < currentTerm
	if args.CandidateTerm < rf.currentTerm {
		reply.CurrentTerm = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	isCandidateValid := !rf.votedFor.hasVoted || rf.votedFor.candidateId == args.CandidateId
	isLogValid := rf.lastAppliedIndex <= args.LastLogIndex && rf.log[rf.lastAppliedIndex].term <= args.LastLogTerm

	if isCandidateValid && isLogValid {
		reply.VoteGranted = true
	}
}
