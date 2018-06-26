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
	"math/rand"
	"sort"
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
	HEARTBEAT = 120 * time.Millisecond
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
	Command interface{}
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
	nextIndex   []int // leader attr
	matchIndex  []int // leader attr

	state           string
	electionTimeout time.Duration
	heartbeat       time.Duration // leader attr
	appEntCh        chan bool
	grantVoteCh     chan bool
	winElectionCh   chan bool // candidate attr
	applyMsgCh      chan ApplyMsg
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

func setRequestVoteReply(reply *RequestVoteReply, term int, grant bool) {
	reply.Term = term
	reply.VoteGranted = grant
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
	Success bool
}

func setAppendEntriesReply(reply *AppendEntriesReply, term int, success bool) {
	reply.Term = term
	reply.Success = success
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs,
	reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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

	rf.mu.Lock()
	if term, isLeader = rf.GetState(); !isLeader {
		rf.mu.Unlock()
		return index, term, false
	}
	index = len(rf.logEntries)
	rf.logEntries = append(rf.logEntries, LogEntry{
		Index: index, 
		Term: term,
		Command: command})
	DPrintf("leader %d at term %d receive command %v", rf.me, rf.currentTerm, command)
	rf.mu.Unlock()
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

//---------- above are some definition of structs and useful functions ----------
//---------- below are functions implement the logic ----------

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// check term
	// DPrintf("%v %v receive request vote args from %d %+v", rf.state, rf.me, args.CandidateId, *args)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		DPrintf("%v %v reject request vote to candidate %d because current term %v > request term %v",
			rf.state, rf.me, args.CandidateId, rf.currentTerm, args.Term)
		setRequestVoteReply(reply, rf.currentTerm, false)
		return
	}
	if args.Term > rf.currentTerm {
		// DPrintf("%v %v become follower and update term from %v to %v because receive request vote args %+v from server %v",
			// rf.state, rf.me, rf.currentTerm, args.Term, args, args.CandidateId)
		rf.becomeFollower()
		rf.votedFor = -1
		rf.currentTerm = args.Term
	}
	// check voted for
	cond1 := rf.votedFor != -1 && rf.votedFor != args.CandidateId
	// check log up-to-date
	lastLog := rf.logEntries[len(rf.logEntries)-1]
	cond2 := len(rf.logEntries) > 1 && lastLog.Term > args.LastLogTerm
	cond3 := len(rf.logEntries) > 1 && lastLog.Term == args.LastLogTerm && 
			lastLog.Index > args.LastLogIndex
	if cond1 || cond2 || cond3 {
		DPrintf("%v %v reject request vote to candidate %d because cond1=%v cond2=%v cond3=%v, rf.votedFor = %d, " + 
			"len(rf.logEntries) = %d, lastLog.Term = %d, lastLog.Index = %d, args = %+v", 
			rf.state, rf.me, args.CandidateId, cond1, cond2, cond3, rf.votedFor, len(rf.logEntries),
			lastLog.Term, lastLog.Index, *args)
		setRequestVoteReply(reply, rf.currentTerm, false)
		return
	}
	// DPrintf("%v %v grant vote for %v at term %v", rf.state, rf.me, args.CandidateId, args.Term)
	rf.votedFor = args.CandidateId
	CleanAndSet(rf.grantVoteCh)
	setRequestVoteReply(reply, rf.currentTerm, true)

	// DPrintf("%v %v send reply %+v to candidate %v", rf.state, rf.me, reply, args.CandidateId)
}

type Counter struct {
	sync.Mutex
	count    int
	reported bool
}

func (rf *Raft) requestVoteWithArgs(server int, args *RequestVoteArgs, counter *Counter) {
	reply := &RequestVoteReply{}
	ok := rf.sendRequestVote(server, args, reply)
	// DPrintf("%v %v receive request vote reply from %v %+v ", rf.state, rf.me, server, *reply)
	if !ok {
		return
	}
	if reply.VoteGranted {
		counter.Lock()
		counter.count++
		if counter.count > len(rf.peers)/2 && !counter.reported {
			DPrintf("candidate %v win election at term %v", rf.me, rf.currentTerm)
			CleanAndSet(rf.winElectionCh)
			counter.reported = true
		}
		counter.Unlock()
	} else {
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.becomeFollower()
			DPrintf("candidate %v become follower and update term from %v to %v because receive request vote reply from %v %+v",
				rf.me, rf.currentTerm, reply.Term, server, *reply)
			rf.currentTerm = reply.Term
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) dispatchRequestVote() {
	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = rf.me
	args := &RequestVoteArgs{rf.currentTerm, rf.me, len(rf.logEntries) - 1,
		rf.logEntries[len(rf.logEntries)-1].Term}
	rf.mu.Unlock()
	// DPrintf("%v %v dispatching request vote args %+v", rf.state, rf.me, *args)
	counter := &Counter{count: 1, reported: false}
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go rf.requestVoteWithArgs(i, args, counter)
	}
}

func (rf *Raft) applyLog() {
	// commit each log
	for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			rf.applyMsgCh <- ApplyMsg{
				CommandValid: true, 
				Command: rf.logEntries[rf.lastApplied].Command,
				CommandIndex: rf.lastApplied}
		}
}

func (rf *Raft) updateCommitIndex() {
	commitIndices := make([]int, len(rf.peers))
	copy(commitIndices, rf.matchIndex)
	rf.matchIndex[rf.me] = len(rf.logEntries) - 1
	sort.Ints(commitIndices)
	N := commitIndices[len(rf.peers) / 2]
	if rf.state == LEADER && rf.commitIndex < N && rf.logEntries[N].Term == rf.currentTerm {
		rf.commitIndex = N
		rf.applyLog()
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		DPrintf("%v %v reject append entries to leader %d because current term %v > request term %v", 
			rf.state, rf.me, args.LeaderId, rf.currentTerm, args.Term)
		setAppendEntriesReply(reply, rf.currentTerm, false)
		return
	}
	// append entry comes from a leader with term >= current term
	CleanAndSet(rf.appEntCh)
	if args.Term > rf.currentTerm {
		DPrintf("%v %v become follower and update term from %v to %v because receive append entry args %+v from leader %v",
			rf.state, rf.me, rf.currentTerm, args.Term, args, args.LeaderId)
		rf.becomeFollower()
		rf.currentTerm = args.Term
	}
	// check rf has the log entry before new ones
	if len(rf.logEntries) < args.PrevLogIndex+1 {
		DPrintf("%v %v reject append entries to leader %d because log length %d < args.prevLogIndex + 1 = %d", 
				rf.state, rf.me, args.LeaderId, len(rf.logEntries), args.PrevLogIndex + 1)
		setAppendEntriesReply(reply, rf.currentTerm, false)
		return
	}
	if rf.logEntries[args.PrevLogIndex].Term != args.PrevLogTerm {
		DPrintf("%v %v reject append entries to leader %d because log entries[%d].Term = %d, args.prevLogTerm = %d", 
			rf.state, rf.me, args.LeaderId, args.PrevLogIndex, rf.logEntries[args.PrevLogIndex].Term, args.PrevLogTerm)
		setAppendEntriesReply(reply, rf.currentTerm, false)
		return
	}
	// find unmatch entries
	i := 0
	for j := args.PrevLogIndex + 1; 
			i < len(args.Entries) && j < len(rf.logEntries); i, j = i + 1, j + 1 {
		// if existing entry doesn't match, delete this one and all after
		if args.Entries[i].Term != rf.logEntries[j].Term {
			rf.logEntries = rf.logEntries[:j]
			break
		}
	}
	// append entries
	if i < len(args.Entries) {
		rf.logEntries = append(rf.logEntries, args.Entries[i:]...)
	}
	if args.LeaderCommit > rf.commitIndex {
		// DPrintf("%v %v update commitIndex from %d to %d because receive append entry args %+v from leader %v",
		// 	rf.state, rf.me, rf.commitIndex, args.LeaderCommit, args, args.LeaderId)
		rf.commitIndex = Minint(args.LeaderCommit, len(rf.logEntries)-1)
		rf.applyLog()
	}
	setAppendEntriesReply(reply, rf.currentTerm, true)
}

func (rf *Raft) appendEntriesWithArgs(server int, args *AppendEntriesArgs) {
	reply := &AppendEntriesReply{}
	ok := rf.sendAppendEntries(server, args, reply)
	if !ok {
		return
	}
	if !reply.Success {
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.becomeFollower()
			DPrintf("leader %v become follower and update term from %v to %v because receive append entry reply %+v from %v",
				rf.me, rf.currentTerm, reply.Term, reply, server)
			rf.currentTerm = reply.Term
		} else { // log entries do not match
			rf.nextIndex[server]--
			DPrintf("leader %v update nextIndex[%d] to %d because log inconsistent", 
				rf.me, server, rf.nextIndex[server])
		}
		rf.mu.Unlock()
	} else {
		rf.mu.Lock()
		rf.nextIndex[server] = rf.nextIndex[server] + len(args.Entries)
		rf.matchIndex[server] = rf.nextIndex[server] - 1
		rf.updateCommitIndex()	
		rf.mu.Unlock()
	}
}

func (rf *Raft) dispatchAppendEntries() {
	rf.mu.Lock()
	term := rf.currentTerm
	leaderId := rf.me
	commitIndex := rf.commitIndex
	logEntries := rf.logEntries
	nextIndex := rf.nextIndex
	rf.mu.Unlock()
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		var entries []LogEntry
		if nextIndex[i] < len(logEntries) {
			entries = logEntries[nextIndex[i]:]
		} else {
			entries = make([]LogEntry, 0)
		}
		lastLog := logEntries[nextIndex[i] - 1]
		args := &AppendEntriesArgs{term, leaderId, lastLog.Index, lastLog.Term, entries, commitIndex}
		go rf.appendEntriesWithArgs(i, args)
	}
}

func (rf *Raft) becomeFollower() {
	// DPrintf("%v %v become follower at term %v", rf.state, rf.me, rf.currentTerm)
	rf.state = FOLLOWER
	rf.resetElectionTimeout()
}

func (rf *Raft) becomeCandidate() {
	// DPrintf("%v %v become candidate at term %v", rf.state, rf.me, rf.currentTerm)
	rf.state = CANDIDATE
	rf.resetElectionTimeout()
}

func (rf *Raft) becomeLeader() {
	if rf.state != CANDIDATE {
		return
	}
	DPrintf("%v %v become leader at term %d", rf.state, rf.me, rf.currentTerm)
	rf.state = LEADER
	rf.heartbeat = HEARTBEAT
	// init nextIndex and matchIndex
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = len(rf.logEntries)
		rf.matchIndex[i] = 0
	}
}

func (rf *Raft) resetElectionTimeout() {
	rand.Seed(time.Now().UnixNano())
	timeout := rand.Int63n(300) + 400
	rf.electionTimeout = time.Duration(timeout) * time.Millisecond
}

func (rf *Raft) run() {
	for {
		rf.mu.Lock()
		state := rf.state
		eto := rf.electionTimeout
		// term := rf.currentTerm
		hb := rf.heartbeat
		rf.mu.Unlock()
		switch state {
		case FOLLOWER:
			timer := time.NewTimer(eto)
			select {
			case <-rf.grantVoteCh:
				// DPrintf("follower %v at term %v receive channel grant vote", rf.me, term)
				timer.Stop()
			case <-rf.appEntCh:
				// DPrintf("follower %v receive channel append entry", rf.me)
				timer.Stop()
			case <-timer.C:
				// DPrintf("follower %v at term %v election time out", rf.me, term)
				rf.mu.Lock()
				rf.becomeCandidate()
				rf.mu.Unlock()
			}
		case CANDIDATE:
			go rf.dispatchRequestVote()
			timer := time.NewTimer(eto)
			select {
			case <-rf.grantVoteCh:
				// DPrintf("candidate %v at term %v receive channel grant vote", rf.me, term)
				timer.Stop()
			case <-rf.appEntCh:
				// DPrintf("candidate %v receive channel append entry ", rf.me)
				timer.Stop()
			case <-rf.winElectionCh:
				// DPrintf("candidate %v at term %v receive channel win election", rf.me, term)
				timer.Stop()
				rf.mu.Lock()
				rf.becomeLeader()
				rf.mu.Unlock()
			case <-timer.C:
				// DPrintf("candidate %v at term %v election time out", rf.me, term)
				rf.mu.Lock()
				rf.resetElectionTimeout()
				rf.mu.Unlock()
			}
		case LEADER:
			rf.dispatchAppendEntries()
			time.Sleep(hb)
		}
	}
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
	for i := 0; i < len(peers); i++ {
		rf.nextIndex[i] = 0
		rf.matchIndex[i] = 0
	}

	rf.grantVoteCh = make(chan bool)
	rf.appEntCh = make(chan bool)
	rf.winElectionCh = make(chan bool)
	rf.applyMsgCh = applyCh

	rf.becomeFollower()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.run()

	return rf
}
