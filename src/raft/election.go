package raft

import "math"

// Implements the election-related functionalities.
// Includes the startElection function and the logic
// for handling votes and becoming a leader.

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
func (rf *Raft) sendRequestVote(server int, voteFor chan int) {
	Debugf("rf: %d - sendRequestVote()\n", rf.me)

	if server == rf.me {
		voteFor <- 1
		return
	}

	args := &RequestVoteArgs{
		CandidateTerm: rf.currentTerm,
		CandidateId:   rf.me,
		LastLogIndex:  len(rf.log),
		LastLogTerm:   len(rf.log),
	}

	reply := &RequestVoteReply{}
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	if ok && reply.VoteGranted {
		voteFor <- 1
	} else {
		if reply.CurrentTerm > rf.currentTerm {
			rf.currentTerm = reply.CurrentTerm
			rf.convertToFollower()
		}
		voteFor <- 0
	}
}

func (rf *Raft) sendVotes() int {
	Debugf("rf: %d - sendVotes()\n", rf.me)

	peers := len(rf.peers)
	votes := make(chan int, peers)

	for server := range len(rf.peers) {
		go rf.sendRequestVote(server, votes)
	}

	totalVotes := 0

	for i := 0; i < peers; i++ {
		totalVotes += <-votes
	}

	Debugf("rf: %d totalVotes %d", rf.me, totalVotes)

	return totalVotes
}

// server starts election for self
// request a vote from every server
// if votes > half, is elected
func (rf *Raft) startElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.resetElectionTimeout()
	Debugf("rf: %d in election\n", rf.me)

	totalVotes := rf.sendVotes()
	Debugf("raft: %d, totalVotes: %d", rf.me, totalVotes)

	// goroutines to send to every server
	// cycle thru every peer except self

	votesNeeded := int(math.Floor(float64(len(rf.peers))/2) + 1)
	if totalVotes >= votesNeeded {
		rf.state = Leader
		rf.sendHeartbeats()
	}

}
