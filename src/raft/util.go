package raft

import "log"

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

var debug bool = true

func Debugf(format string, v ...interface{}) {
	if debug {
		log.Printf(format, v...)
	}
}
