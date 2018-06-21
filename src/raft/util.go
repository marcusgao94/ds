package raft

import (
	"log"
)

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func Minint(a, b int) int {
	if a <= b {
		return a
	}
	return b
}

func CleanAndSet(ch chan bool) {
	for {
		select {
		case <-ch:
		default:
			break
		}
	}
	ch <- true
}
