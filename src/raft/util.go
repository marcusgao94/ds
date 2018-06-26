package raft

import (
	"fmt"
	"time"
)

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		formatted := fmt.Sprintf(format, a...)
		fmt.Printf("%v: %s\n", time.Now().Format("2006-01-02 15:04:05.999999"), formatted)
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
LOOP:
	for {
		select {
		case <-ch:
		default:
			break LOOP
		}
	}
	select {
	case ch <- true:
	default:
	}
}
