package raft

import (
	"time"
)

func (rf *Raft) mainLoop() {

	// while not dead
	// if state == Leader, send out heart beats
	// if state == Follower, if havent heard from leader in time start vote
	for {
		time.Sleep(10 * time.Millisecond)
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()

		switch state {
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
