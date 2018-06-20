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
	"math"
	"math/rand"
	"sync"
	"time"
)
import "labrpc"

// import "bytes"
// import "labgob"

const (
	CANDIDATE = "candidate"
	LEADER    = "leader"
	FOLLOWER  = "follower"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Index   int
	Term    int
	Command string
}

type Timer struct {
	lastTime   int64 // milliseconds since epoch
	timeout    int64 // timeout in milliseconds
	stopSignal chan bool
}

func (tm *Timer) reset(timeout int64, callback func()) {
	tm.stopSignal <- true
	tm.timeout = timeout
	go func(callback func()) {
		tm.lastTime = NowMilli()
		for {
			select {
			case <-tm.stopSignal:
				return
			default:
				if now := NowMilli(); now-tm.lastTime >= tm.timeout {
					callback()
					return
				} else {
					time.Sleep(10 * time.Millisecond)
				}
			}
		}
	}(callback)
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	logEntries  []LogEntry
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int

	state string
	timer *Timer
}

func (rf *Raft) resetElectionTimeout() {
	rand.Seed(time.Now().UnixNano())
	timeout := rand.Int63n(300) + 300
	rf.timer.reset(timeout, rf.becomeCandidate)
}

func (rf *Raft) resetHeartbeat() {
	timeout := 120
	// rf.timer.reset(timeout, )
}

func (rf *Raft) becomeCandidate() {
	rf.state = CANDIDATE
	rf.currentTerm++
	rf.resetElectionTimeout()
	for i := 0; i < len(rf.logEntries; i++) {
		req := RequestVoteArgs{rf.currentTerm, rf.me, len(rf.logEntries) - 1,
												 	 rf.logEntries[len(rf.logEntries)-1]}
		rep := RequestVoteReply{}
		rf.sendRequestVote(i, req, rep)
	}
}

func (rf *Raft) becomeFollower() {
	rf.state = FOLLOWER
	rf.resetElectionTimeout()
}

func (rf *Raft) becomeLeader() {
	rf.state = LEADER
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.state == LEADER
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
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

//
// restore previously persisted state.
//
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

func setReply(reply interface{}, term int, success bool) {
	reply.Term = term
	switch reply.(type) {
	case *RequestVoteReply:
		reply.voteGranted = success
	case *AppendEntriesReply:
		reply.success = success
	default:
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// check term
	if args.Term < rf.currentTerm {
		setReply(reply, rf.currentTerm, false)
		return
	}
	if args.Term > rf.currentTerm {
		rf.votedFor = -1
	}
	// check voted for
	cond1 := rf.votedFor != -1 && rf.votedFor != args.CandidateId
	// check log up-to-date
	lastLog := rf.logEntries[len(rf.logEntries)-1]
	cond2 := lastLog.Term > args.LastLogTerm
	cond3 := lastLog.Term == args.LastLogTerm && lastLog.Index > args.LastLogIndex
	if cond1 || cond2 || cond3 {
		setReply(reply, rf.currentTerm, false)
		return
	}
	rf.votedFor = args.CandidateId
	rf.currentTerm = args.Term
	rf.resetElectionTimeoutTimer()
	setReply(reply, rf.currentTerm, true)
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if args.Term < rf.currentTerm {
		setReply(reply, rf.currentTerm, false)
		return
	}
	// check rf has the log entry before new ones
	if len(rf.logEntries) < args.PrevLogIndex+1 ||
		rf.logEntries[args.PrevLogIndex].Term != args.PrevLogTerm {
		setReply(reply, rf.currentTerm, false)
		return
	}
	rf.resetElectionTimeout()
	i := 0
	for j := args.PrevLogIndex + 1; i < len(args.Entries) && j < len(rf.logEntries); i++ {
		// if existing entry doesn't match, delete this one and all after
		if args.Entries[i].Term != rf.logEntries[j].Term {
			rf.logEntries = rf.logEntries[:j]
			break
		}
		j++
	}
	// append entries
	for ; i < len(args.Entries); i++ {
		rf.logEntries = append(rf.logEntries, args.Entries[i])
	}
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = math.Min(args.LeaderCommit, len(rf.logEntries)-1)
	}

}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logEntries = append(make([]LogEntry, 0), LogEntry{})
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.timer = &Timer{NowMilli(), 0, make(chan bool)}
	rf.becomeFollower()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
