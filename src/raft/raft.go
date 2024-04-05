package raft

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
	"sync"
	"sync/atomic"
	"time"

	"6.824/labrpc"
)

// import "bytes"
// import "../labgob"

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

type LogEntry struct {
	Term    int
	Command interface{}
}

type State int

const (
	Follower = iota
	Candidate
	Leader
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// persistent state
	currentTerm int        // latest term server has seen; 0 default
	votedFor    int        // candidateId recieved vote in cur term; -1 if none
	logs        []LogEntry // first index 1
	// logIndex of len(logs) - 1

	// volatile state
	commitIndex int // highest log entry known
	lastApplied int // index of highest log entry applied to state machine

	state               State
	lastHeardFromLeader time.Time

	// leader only, volatile state
	// contains information about follower servers
	nextIndex []int // index of next log entry on server i; init to len(rf.logs) + 1
	applyCh   chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool

	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == Leader
	rf.mu.Unlock()

	return term, isleader
}

func (rf *Raft) getState() State {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state
}

func (rf *Raft) getTimeLastHeardFromLeader() time.Time {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.lastHeardFromLeader
}

// leaderLoop: send heartbeats
func (rf *Raft) leaderLoop() {
	for !rf.killed() && rf.getState() == Leader {
		time.Sleep(rf.getHeartbeatTimeout())
		rf.sendHeartbeats()

		time.Sleep(10 * time.Millisecond)
	}
}

// followerLoop: start election if too much time has passed
func (rf *Raft) followerLoop() {
	for !rf.killed() && rf.getState() != Leader {
		electionTimeout := rf.getElectionTimeout()
		time.Sleep(electionTimeout)

		// if follower hasn't hear from leader during this time call election
		if time.Since(rf.getTimeLastHeardFromLeader()) > electionTimeout {
			rf.startElection()
		}

		time.Sleep(10 * time.Millisecond)
	}
}

// mainLoop
func (rf *Raft) mainLoop() {
	DPrintf("raft %d initiated", rf.me)
	for !rf.killed() {
		switch rf.getState() {
		case Leader:
			rf.leaderLoop()
		default:
			rf.followerLoop()
		}

		time.Sleep(10 * time.Millisecond)
	}
	DPrintf("raft %d: died", rf.me)
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
	rf := &Raft{
		peers:       peers,
		persister:   persister,
		me:          me,
		state:       Follower,
		votedFor:    -1,
		currentTerm: 0,
		commitIndex: 0,
		lastApplied: 0,
		applyCh: applyCh,
	}
	rf.logs = append(rf.logs, LogEntry{Term: 0})

	// Your initialization code here (2A, 2B, 2C).
	go rf.mainLoop()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

// Start()
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log.
// start the agreement and return immediately.
// no guarantee command will be committed to the Raft log
//
// index (int): index the command will appear if commited. len(rf.logs) + 1
// term (int): current term
// isLeader (bool): if server is leader
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false

	if rf.getState() != Leader {
		return index, term, isLeader
	}

	rf.mu.Lock()
	term = rf.currentTerm
	rf.logs = append(rf.logs, LogEntry{Term: term, Command: command}) // append command to log
	index = len(rf.logs)
	isLeader = true
	rf.mu.Unlock()

	// fire off AppendEntries
	go rf.sendLogEntries()

	return index, term, isLeader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
//lint:ignore U1000 Ignore unused function temporarily for debugging
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
//
//lint:ignore U1000 Ignore unused function temporarily for debugging
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

// You may want to call in all loops, to avoid having dead Raft
// instances print confusing messages.
func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}
