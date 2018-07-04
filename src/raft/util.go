package raft

import (
	"fmt"
	"time"
)

// Debugging
const Debug = 1

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

func searchFirstLogInTerm(logs []LogEntry, term int) int {
	lo, hi := 1, len(logs)-1
	for lo < hi {
		mid := lo + (hi-lo)/2
		if logs[mid].Term < term {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	// all logs < term or all logs > term
	if logs[lo].Term != term {
		return -1
	}
	return lo
}

func searchLastLogInTerm(logs []LogEntry, term int) int {
	lo, hi := 1, len(logs)-1
	for lo < hi {
		mid := lo + (hi-lo)/2 + 1
		if logs[mid].Term <= term {
			lo = mid
		} else {
			hi = mid - 1
		}
	}
	if logs[lo].Term != term {
		return -1
	}
	return lo
}
