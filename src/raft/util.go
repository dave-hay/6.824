package raft

import (
	"log"
	"math/rand"
	"time"
)

// getHeartbeatTimeout
func (rf *Raft) getHeartbeatTimeout() time.Duration {
	return time.Duration(rand.Intn(100)+100) * time.Millisecond
}

func (rf *Raft) getElectionTimeout() time.Duration {
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

func DPrintln(text string) (n int, err error) {
	if Debug > 0 {
		log.Println(text)
	}
	return
}
