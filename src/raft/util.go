package raft

import (
	"log"
	"time"
)

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func NowMilli() int64 {
	return int64(time.Now().UnixNano() / 1000000)
}

func Minint(a, b int) int {
	if a <= b {
		return a
	}
	return b
}
