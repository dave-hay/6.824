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

	Debugf("rf: %d, request by %d\n", rf.me, args.CandidateId)
	// reply false if term < currentTerm
	if args.CandidateTerm < rf.currentTerm {
		reply.CurrentTerm = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// if candidate hasn't voted or has already voted for candidate
	isCandidateValid := rf.votedFor != -1 || rf.votedFor == args.CandidateId
	isLogValid := rf.lastAppliedIndex <= args.LastLogIndex && len(rf.log) <= args.LastLogTerm

	if isCandidateValid && isLogValid {
		Debugf("rf: %d, request by %d, vote granted %v\n", rf.me, args.CandidateId, reply.VoteGranted)
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

// Receiver implementation:
// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
// 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
// 4. Append any new entries not already in the log
// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// this should reset the timeouts for the nodes

	// 1. Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// If serverTerm >= currentTerm becomes follower
	rf.currentTerm = args.Term
	rf.convertToFollower()
	reply.Success = true

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit
	}

	// if no Entries empty then it is a heatbeat
	// if len(args.Entries) == 0 {

	// }
	//
}
