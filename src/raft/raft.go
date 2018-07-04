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
	"bytes"

	"math/rand"
	"sort"
	"sync"
	"time"
)

import (
	"labgob"
	"labrpc"
)

const (
	CANDIDATE = "candidate"
	LEADER    = "leader"
	FOLLOWER  = "follower"
	HEARTBEAT = 101 * time.Millisecond
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
	w := new(bytes.Buffer)
	enc := labgob.NewEncoder(w)
	enc.Encode(rf.currentTerm)
	enc.Encode(rf.votedFor)
	enc.Encode(rf.logEntries)
	// DPrintf("server %d persist current term %d, votedFor %d, %d logs",
	// rf.me, rf.currentTerm, rf.votedFor, len(rf.logEntries))
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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
	r := bytes.NewBuffer(data)
	dec := labgob.NewDecoder(r)
	var ct, vf int
	var le []LogEntry
	dec.Decode(&ct)
	dec.Decode(&vf)
	dec.Decode(&le)
	rf.currentTerm = ct
	rf.votedFor = vf
	rf.logEntries = le
	// DPrintf("server %d read from persist current term %d, votedFor %d, %d logs",
	// rf.me, rf.currentTerm, rf.votedFor, len(rf.logEntries))
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
	Term          int
	Success       bool
	ConflictTerm  int
	ConflictIndex int
}

func setAppendEntriesReply(reply *AppendEntriesReply, term int, success bool, params ...int) {
	reply.Term = term
	reply.Success = success
	if len(params) == 2 {
		reply.ConflictTerm = params[0]
		reply.ConflictIndex = params[1]
	}
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
		Index:   index,
		Term:    term,
		Command: command})
	rf.persist()
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		// DPrintf("%v %v reject request vote to candidate %d: current term %v > request term %v",
		// rf.state, rf.me, args.CandidateId, rf.currentTerm, args.Term)
		setRequestVoteReply(reply, rf.currentTerm, false)
		return
	}
	if args.Term > rf.currentTerm {
		// DPrintf("%v %v become follower and update term from %v to %v: receive request vote from candidate %v",
		// rf.state, rf.me, rf.currentTerm, args.Term, args.CandidateId)
		rf.becomeFollower()
		rf.votedFor = -1
		rf.currentTerm = args.Term
		rf.persist()
	}
	var cond1, cond2, cond3 bool
	// check voted for
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		cond1 = true
		// DPrintf("%v %v reject request vote to candidate %d: %v %v has already voted for %d",
		// rf.state, rf.me, args.CandidateId, rf.state, rf.me, rf.votedFor)
	}
	// check log up-to-date
	lastLog := rf.logEntries[len(rf.logEntries)-1]
	if len(rf.logEntries) > 1 && lastLog.Term > args.LastLogTerm {
		// DPrintf("%v %v reject request vote to candidate %d: last log term %d > args.term %d",
		// rf.state, rf.me, args.CandidateId, lastLog.Term, args.LastLogTerm)
		cond2 = true
	}
	if len(rf.logEntries) > 1 && lastLog.Term == args.LastLogTerm &&
		lastLog.Index > args.LastLogIndex {
		// DPrintf("%v %v reject request vote to candidate %d: last log index %d > args.index %d",
		// rf.state, rf.me, args.CandidateId, lastLog.Index, args.LastLogIndex)
		cond3 = true
	}
	if cond1 || cond2 || cond3 {
		setRequestVoteReply(reply, rf.currentTerm, false)
		return
	}
	// DPrintf("%v %d grant vote to candidate %d at term %d", rf.state, rf.me, args.CandidateId, args.Term)
	CleanAndSet(rf.grantVoteCh)
	rf.votedFor = args.CandidateId
	rf.persist()
	setRequestVoteReply(reply, rf.currentTerm, true)

}

type Counter struct {
	sync.Mutex
	count    int
	reported bool
}

func (rf *Raft) dispatchRequestVote() {
	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()
	rf.mu.Unlock()
	counter := &Counter{count: 1, reported: false}
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(server int, counter *Counter) {
			rf.mu.Lock()
			args := &RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: len(rf.logEntries) - 1,
				LastLogTerm:  rf.logEntries[len(rf.logEntries)-1].Term}
			rf.mu.Unlock()
			reply := &RequestVoteReply{}

			// send request vote RPC
			ok := rf.sendRequestVote(server, args, reply)
			if !ok {
				return
			}

			// process reply
			if rf.currentTerm != args.Term {
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
					DPrintf("candidate %v become follower and update term from %v to %v: "+
						"receive request vote reply from %v",
						rf.me, rf.currentTerm, reply.Term, server)
					rf.currentTerm = reply.Term
					rf.persist()
				}
				rf.mu.Unlock()
			}
		}(i, counter)
	}
}

func (rf *Raft) applyLog() {
	// commit each log
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		rf.applyMsgCh <- ApplyMsg{
			CommandValid: true,
			Command:      rf.logEntries[rf.lastApplied].Command,
			CommandIndex: rf.lastApplied}
	}
}

func (rf *Raft) updateCommitIndex() {
	commitIndices := make([]int, len(rf.peers))
	copy(commitIndices, rf.matchIndex)
	rf.matchIndex[rf.me] = len(rf.logEntries) - 1
	sort.Ints(commitIndices)
	N := commitIndices[len(rf.peers)/2]
	if rf.state == LEADER && rf.commitIndex < N && rf.logEntries[N].Term == rf.currentTerm {
		// DPrintf("leader %d update commit index from %d to %d", rf.me, rf.commitIndex, N)
		rf.commitIndex = N
		rf.applyLog()
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		// DPrintf("%v %v reject append entries to leader %d: current term %v > request term %v",
		// rf.state, rf.me, args.LeaderId, rf.currentTerm, args.Term)
		setAppendEntriesReply(reply, rf.currentTerm, false, 0, 0)
		return
	}
	// append entry comes from a leader with term >= current term
	CleanAndSet(rf.appEntCh)
	if args.Term > rf.currentTerm {
		// DPrintf("%v %d update term from %v to %v: "+
		// "receive append entry from leader %v",
		// rf.state, rf.me, rf.currentTerm, args.Term, args.LeaderId)
		rf.becomeFollower()
		rf.currentTerm = args.Term
		rf.persist()
	}

	// check rf has the log entry before new ones
	if len(rf.logEntries) < args.PrevLogIndex+1 {
		// DPrintf("%v %v reject append entries to leader %d: log length %d < args.prevLogIndex + 1 = %d",
		// rf.state, rf.me, args.LeaderId, len(rf.logEntries), args.PrevLogIndex+1)
		setAppendEntriesReply(reply, rf.currentTerm, false, -1, len(rf.logEntries))
		return
	}
	if rf.logEntries[args.PrevLogIndex].Term != args.PrevLogTerm {
		// DPrintf("%v %v reject append entries to leader %d: "+
		// "log entries[%d].Term = %d, args.prevLogTerm = %d",
		// rf.state, rf.me, args.LeaderId, args.PrevLogIndex, rf.logEntries[args.PrevLogIndex].Term,
		// args.PrevLogTerm)
		setAppendEntriesReply(reply, rf.currentTerm, false, rf.logEntries[args.PrevLogIndex].Term,
			searchFirstLogInTerm(rf.logEntries, rf.logEntries[args.PrevLogIndex].Term))
		return
	}
	// find unmatch entries
	i := 0
	for j := args.PrevLogIndex + 1; i < len(args.Entries) && j < len(rf.logEntries); i, j = i+1, j+1 {
		// if existing entry doesn't match, delete this one and all after
		if args.Entries[i].Term != rf.logEntries[j].Term {
			// DPrintf("%v %d's log unmatch with term %d leader %d's log at index %d: self log term %d, leader log term %d",
			// rf.state, rf.me, args.Term, args.LeaderId, j, rf.logEntries[j].Term, args.Entries[i].Term)
			rf.logEntries = rf.logEntries[:j]
			break
		}
	}
	// append entries
	if i < len(args.Entries) {
		// DPrintf("%v %d upsert entries index %d to %d: receive append entries from leader %d at term %d",
		// 	rf.state, rf.me, len(rf.logEntries)-1, len(rf.logEntries)+len(args.Entries[i:])-1, args.LeaderId, args.Term)
		rf.logEntries = append(rf.logEntries, args.Entries[i:]...)
	}
	rf.persist()
	if args.LeaderCommit > rf.commitIndex {
		// DPrintf("%v %d update commit index from %d to %d: receive append entries from leader %d at term %d",
		// rf.state, rf.me, rf.commitIndex, Minint(args.LeaderCommit, len(rf.logEntries)-1), args.LeaderId, args.Term)
		rf.commitIndex = Minint(args.LeaderCommit, len(rf.logEntries)-1)
		rf.applyLog()
	}
	setAppendEntriesReply(reply, rf.currentTerm, true)
}

func (rf *Raft) dispatchAppendEntries() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(server int) {
			rf.mu.Lock()
			if rf.state != LEADER {
				rf.mu.Unlock()
				return
			}
			var entries []LogEntry
			if rf.nextIndex[server] < len(rf.logEntries) {
				entries = rf.logEntries[rf.nextIndex[server]:]
			} else {
				entries = make([]LogEntry, 0)
			}
			lastLog := rf.logEntries[rf.nextIndex[server]-1]
			args := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: lastLog.Index,
				PrevLogTerm:  lastLog.Term,
				Entries:      entries,
				LeaderCommit: rf.commitIndex}
			rf.mu.Unlock()
			reply := &AppendEntriesReply{}

			// send append entries RPC
			ok := rf.sendAppendEntries(server, args, reply)
			if !ok {
				return
			}

			// process reply
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.currentTerm != args.Term {
				return
			}
			if !reply.Success {
				if reply.Term > rf.currentTerm {
					rf.becomeFollower()
					DPrintf("leader %d become follower and update term from %v to %v: "+
						"receive append entry reply from %d",
						rf.me, rf.currentTerm, reply.Term, server)
					rf.currentTerm = reply.Term
					rf.persist()
				} else { // log entries do not match
					if reply.ConflictTerm == -1 {
						rf.nextIndex[server] = reply.ConflictIndex
					} else {
						idx := searchLastLogInTerm(rf.logEntries, reply.ConflictTerm)
						if idx == -1 { // do not have conflict term in logs
							rf.nextIndex[server] = reply.ConflictIndex
						} else { // have conflict term in logs, last entry is at idx
							rf.nextIndex[server] = idx + 1
						}
					}
				}
			} else {
				rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
				rf.matchIndex[server] = rf.nextIndex[server] - 1
				rf.updateCommitIndex()
			}
		}(i)
	}
}

func (rf *Raft) becomeFollower() {
	rf.state = FOLLOWER
	rf.resetElectionTimeout()
}

func (rf *Raft) becomeCandidate() {
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
				timer.Stop()
			case <-rf.appEntCh:
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
				timer.Stop()
			case <-rf.appEntCh:
				timer.Stop()
			case <-rf.winElectionCh:
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
