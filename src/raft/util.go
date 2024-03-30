package raft

import (
	"log"
	"math/rand"
	"time"
)

// Time
func (rf *Raft) setHeartbeatTimeout() time.Duration {
	return time.Duration(rand.Intn(150)+150) * time.Millisecond
}

func (rf *Raft) setElectionTimeout() time.Duration {
	return time.Duration(rand.Intn(200)+400) * time.Millisecond
}

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

var debug bool = false

func Debugf(format string, v ...interface{}) {
	if debug {
		log.Printf(format, v...)
	}
}
