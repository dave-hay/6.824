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

// AppendEntries RPC struct
// (though you may not need all the arguments yet),
// and have the leader send them out periodically.
type AppendEntriesArgs struct {
	Term         int   // leaders term
	LeaderId     int   // so followers can redirect clients
	PrevLogIndex int   // index of log entry immediately preceding new ones
	PrevLogTerm  int   // term of prevLogIndex entry
	Entries      []Log // log entries to store (empty for hearbeat; may send > 1 for efficiency)
	LeaderCommit int   // leaders commit index
}

type AppendEntriesReply struct {
	Term    int  // curTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex + prevLogTerm
}

// Write an AppendEntries RPC handler method
// resets the election timeout so other servers
// don't step forward as leaders while one is elected

// invoked by leader to replicate log entries
// also used as heartbeat
// TODO:
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// this should reset the timeouts for the nodes

	// if no Entries empty then it is a heatbeat
	if len(args.Entries) == 0 {

	}
	//
}
