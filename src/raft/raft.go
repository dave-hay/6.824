package raft

//lint:ignore U1000 Ignore unused function temporarily for debugging

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"log"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labrpc"
)

// import "bytes"
// import "6.824/labgob"

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type VotedFor struct {
	candidateId int
	hasVoted    bool
}

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

// A Go object implementing a single Raft peer.
// Based on Figure 2
type Raft struct {
	mu              sync.Mutex          // Lock to protect shared access to this peer's state
	peers           []*labrpc.ClientEnd // RPC end points of all peers
	persister       *Persister          // Object to hold this peer's persisted state
	me              int                 // this peer's index into peers[]
	dead            int32               // set by Kill()
	state           State
	electionTimeout time.Time

	// persistent state, all servers
	currentTerm int      // latest term server has seen; initialized to 0
	votedFor    VotedFor // candidateId that recived vote in curTerm or null
	log         []Log    // log entries; first index is 1

	// volatile state, all servers

	// index of highest log entry known to be committed
	// initialized to 0, increasing monotonically
	commitIndex int

	// index of highest log entry applied to state machine
	// initialized to 0, increasing monotonically
	lastAppliedIndex int

	// TODO: reinitialized after election
	// volatile state, leaders
	// tracking the state of other servers

	// index of the next log entry to send to each server
	// initialized to leader last log index + 1
	nextIndex []int

	// index of highest log entry know to be replicated on each server
	// initialized to 0, increasing monotonically
	matchIndex []int
}

type Log struct {
	Command string // command for state machine
	Term    int    // term when entry was recieved by leader; first index is 1
}

func (rf *Raft) sendAppendEntries(server int, isHeartbeat bool) {
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: len(rf.log),
		PrevLogTerm:  len(rf.log),
		LeaderCommit: rf.commitIndex,
	}

	if isHeartbeat {
		args.Entries = make([]Log, 0)
	}

	reply := &AppendEntriesReply{}

	ok := rf.peers[server].Call("Raft.AppendEntries", &args, reply)

	if !ok {
		return
	}

	if reply.Success {
		// Update nextIndex and matchIndex for the follower
		// Check if we can commit new entries
	} else if reply.Term > rf.currentTerm {
		rf.convertToFollower()
	}
}

func (rf *Raft) sendHeartbeats() {
	for server := range len(rf.peers) {
		if server != rf.me {
			go rf.sendAppendEntries(server, true)
		}
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isLeader := rf.state == Leader
	return term, isLeader
}

// Sets state to Follower.
// Resets voted for and election timeout
func (rf *Raft) convertToFollower() {
	rf.state = Follower
	rf.votedFor = VotedFor{-1, false}
	rf.resetElectionTimeout()
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
func (rf *Raft) sendRequestVote(server int, voteFor chan int) {
	Debugf("rf: %d - sendRequestVote()\n", rf.me)

	// if server == rf.me {
	// 	voteFor <- 1
	// 	return
	// }

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
		if server != rf.me {
			go rf.sendRequestVote(server, votes)
		}
	}

	totalVotes := 1

	for i := 0; i < peers-1; i++ {
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
	rf.votedFor = VotedFor{rf.me, true}
	rf.resetElectionTimeout()
	Debugf("rf: %d in election\n", rf.me)

	totalVotes := rf.sendVotes()
	Debugf("raft: %d, totalVotes: %d", rf.me, totalVotes)

	// goroutines to send to every server
	// cycle thru every peer except self

	votesNeeded := int(math.Floor(float64(len(rf.peers))/2) + 1)
	if totalVotes >= votesNeeded {
		// TODO: election is won
		rf.state = Leader
		rf.sendHeartbeats()
	}

}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) setHeartbeatTimeout() time.Duration {
	return time.Duration(rand.Intn(150)+150) * time.Millisecond
}

func (rf *Raft) setElectionTimeout() time.Duration {
	return time.Duration(rand.Intn(200)+400) * time.Millisecond
}

// Record the current time as the time when the election timeout should be reset.
func (rf *Raft) resetElectionTimeout() {
	rf.electionTimeout = time.Now()
}

func (rf *Raft) timeSinceElectionTimeout() time.Duration {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return time.Since(rf.electionTimeout)
}

func (rf *Raft) mainLoop() {

	// while not dead
	// if state == Leader, send out heart beats
	// if state == Follower, if havent heard from leader in time start vote
	for {

		switch rf.state {
		case Leader:
			time.Sleep(rf.setHeartbeatTimeout())
			rf.sendHeartbeats()
		default:
			timeOutlength := rf.setElectionTimeout()
			time.Sleep(timeOutlength)
			if timeOutlength < rf.timeSinceElectionTimeout() {
				Debugf("rf: %d starting election\n", rf.me)
				rf.startElection()
				Debugf("rf: %d end of election\n", rf.me)
			}
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.state = Follower

	if debug {
		log.SetFlags(log.Ldate | log.Lmicroseconds | log.Lshortfile)
		log.Println("debugging enabled")
	}

	// create a background goroutine that will
	// kick off leader election periodically by sending out RequestVote RPCs
	// when it hasn't heard from another peer for a while. This way a peer
	// will learn who is the leader, if there is already a leader, or become the leader itself.

	rf.resetElectionTimeout()
	go rf.mainLoop()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

var debug bool = false

func Debugf(format string, v ...interface{}) {
	if debug {
		log.Printf(format, v...)
	}
}

// 2C

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}
