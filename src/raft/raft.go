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
	"labgob"
	"labrpc"
	"math/rand"
	"sort"
	"sync"
	"time"
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
	CommandValid bool // false means use snapshots
	Command      interface{}
	CommandIndex int
	Snapshot     []byte
}

type RaftState string

const (
	Leader    RaftState = "Leader"
	Candidate           = "Candidate"
	Follower            = "Follower"
)

// define a struct to hold information about each log entry
type LogEntry struct {
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

	state        RaftState     // the state(Leader, Candidate or Follower)
	resetTimerCh chan struct{} // the chan for reset electionTimer

	// persistent state on servers
	currentTerm   int        // lateset term server has seen(initialized to 0 on first boot, increases monotonically)
	votedFor      int        // candidateId that received vote in current term(or null if none)
	logs          []LogEntry //log entries, each entry contains command for state machine, and term when entry was received by leader(first index is 1)
	snapshotIndex int
	snapshotTerm  int

	// volatile state on all servers
	commitIndex int // index of highest log entry known to be committed(initialized to 0, increases monotonically)
	lastApplied int // index of highest log entry applied to state machine(initialized to 0, increases monotonically)

	// volatile state on Leader
	nextIndex  []int // for each sercer, index of the next log entry to send to that server(initialized to leader last log index + 1)
	matchIndex []int // for each sercer, index of the highest log entry known to be replicated on that server(initialized to 0, increases monotonically)

	applyCh    chan ApplyMsg // applyCh is a channel on which the tester or service expects Raft to send ApplyMsg messages.
	commitCond *sync.Cond
	shutdownCh chan struct{} // shutdown channel, shut raft instance gracefully
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate's term
	CandidateId  int // candidate who is requesting vote
	LastLogIndex int // the index of candidate's last log entry
	LastLogTerm  int // the index of candidate's last log entry
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
// AppendEntries RPC arguments structure.
// field names must start with capital letters!
//
type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeaderId     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store(empty for heartbeats; may send more than one for efficiency)
	LeaderCommit int        // leader's commitIndex
}

//
// AppendEntries RPC reply structure.
// field names must start with capital letters!
//
type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry prevLogIndex and prevLogTerm

	// extra feedback information from follower to leader
	NextIndex int
	NextTerm  int
}

//
// InstallSnapshot RPC
//
type InstallSnapshotArgs struct {
	Term              int // leader's Term
	LeaderID          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Snapshot          []byte
}

type InstallSnapshotReply struct {
	CurrentTerm int
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

//
// should be called when holding the lock
// return lastLogIndex lastLogTerm,
//
func (rf *Raft) getLastLogState() (int, int) {
	lastLogIndex := rf.snapshotIndex + len(rf.logs) - 1
	lastLogTerm := rf.logs[len(rf.logs)-1].Term
	return lastLogIndex, lastLogTerm
}

// should be called when holding the lock
func (rf *Raft) turnToFollower() {
	DPrintf("Raft server %d turn from %s to Follower\n", rf.me, rf.state)
	rf.state = Follower
	rf.votedFor = -1
}

//
// example RequestVote RPC handler.
// Implement the RequestVote() RPC handler so that servers will vote for one another.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	select {
	case <-rf.shutdownCh:
		DPrintf("[%d-%s]: peer %d is shutting down, reject RV rpc request.\n", rf.me, rf.state, rf.me)
		return
	default:
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	lastLogIndex, lastLogTerm := rf.getLastLogState()

	DPrintf("[%d-%s]: rpc RequestVote, from peer: %d, arg.term: %d, my.term: %d (last log idx: %d->%d, term: %d->%d)",
		rf.me, rf.state, args.CandidateId, args.Term, rf.currentTerm, args.LastLogIndex,
		lastLogIndex, args.LastLogTerm, lastLogTerm)

	var voteGranted bool = true

	// update the rf.currentTerm
	if args.Term > rf.currentTerm {
		rf.turnToFollower()
		rf.currentTerm = args.Term
	}

	if rf.votedFor != -1 {
		DPrintf("%d 拒绝了%d 的投票，因为已经投给%d\n", rf.me, args.CandidateId, rf.votedFor)
		voteGranted = false
	}

	if args.Term < rf.currentTerm {
		DPrintf("%d(%d) 拒绝了%d(%d) 的投票，因为任期落后\n", rf.me, rf.currentTerm, args.CandidateId, args.Term)
		voteGranted = false
	}

	if args.LastLogTerm < lastLogTerm ||
		args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex {
		DPrintf("%d拒绝了%d的投票，因为not up-to-data\n", rf.me, args.CandidateId)
		voteGranted = false
	}

	if voteGranted == true {
		rf.resetTimerCh <- struct{}{}
		rf.votedFor = args.CandidateId
		rf.state = Follower
		DPrintf("[%d-%s]: peer %d vote to peer %d (last log idx: %d->%d, term: %d->%d)\n",
			rf.me, rf.state, rf.me, args.CandidateId, args.LastLogIndex, lastLogIndex, args.LastLogTerm, lastLogTerm)
	}

	// if args.Term > rf.currentTerm {
	// 	rf.currentTerm = args.Term
	// }

	// prepare for RequestVoteReply
	reply.VoteGranted = voteGranted
	reply.Term = rf.currentTerm

	rf.persist()

	return
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

//
// AppenEntries RPC handler.
// Log replication or heartbeat
//
func (rf *Raft) AppenEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	select {
	case <-rf.shutdownCh:
		DPrintf("[%d-%s]: peer %d is shutting down, reject AE rpc request.\n", rf.me, rf.state, rf.me)
		return
	default:
	}

	DPrintf("[%d-%s]: rpc AppenEntries, from peer: %d, term: %d\n", rf.me, rf.state, args.LeaderId, args.Term)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Term check
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// Pass the term check
	// Reset election timer (heartbeat routine)
	rf.resetTimerCh <- struct{}{}

	// update self's stale state
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.turnToFollower()

		if rf.votedFor != args.LeaderId {
			rf.votedFor = args.LeaderId
		}
	}

	// ======= 2B code start here ======
	reply.Term = rf.currentTerm
	prevLogIndex, prevLogTerm := rf.getLastLogState()

	if args.PrevLogIndex > prevLogIndex {
		// leader's log is longer than server
		// so missmatch

		reply.Success = false
		reply.NextIndex = prevLogIndex + 1
		reply.NextTerm = prevLogTerm

		DPrintf("%d拒绝了%d(任期:%d)的AppendEntries, 因为server的Log少于Leader, 需要在下次heartbeat从(%d, %d)开始发送Log！", rf.me, args.LeaderId, reply.Term, reply.NextIndex, reply.NextTerm)
		rf.persist()
		return
	}

	// if prevLogIndex < rf.snapshotIndex {
	// 	fmt.Printf("[%d-%s] prevLogIndex(%d), rf.snapshotIndex(%d), len(logs) = %d\n", rf.me, rf.state, prevLogIndex, rf.snapshotIndex, len(rf.logs))
	// 	panic("prevLogIndex < rf.snapshotIndex(%d)")
	// } else {
	// 	fmt.Printf("[%d-%s] prevLogIndex(%d), rf.snapshotIndex(%d), len(logs) = %d\n", rf.me, rf.state, prevLogIndex, rf.snapshotIndex, len(rf.logs))
	// }

	if args.PrevLogIndex < rf.snapshotIndex {
		reply.Success = true
		reply.NextIndex = rf.snapshotIndex + 1
		reply.NextTerm = rf.logs[0].Term
		return
	}

	if args.PrevLogIndex < prevLogIndex {
		// leader's log is shorter than server
		// so the server need to truncate the log
		prevLogIndex = args.PrevLogIndex
		prevLogTerm = rf.logs[prevLogIndex-rf.snapshotIndex].Term
	}

	// now the prevLogIndex is matched
	// so we need to check whether the prevLog's term is match
	if args.PrevLogTerm == prevLogTerm {
		// the term is match
		reply.Success = true
		rf.logs = rf.logs[:prevLogIndex+1-rf.snapshotIndex]
		rf.logs = append(rf.logs, args.Entries...)

		// tell the leader the matchIndex
		newMatchLogIndex, newMatchLogTerm := rf.getLastLogState()
		reply.NextIndex = newMatchLogIndex + 1
		reply.NextTerm = newMatchLogTerm

		// commit
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, newMatchLogIndex)
			// signal possible update commit index (for follower)
			go func() { rf.commitCond.Signal() }()
		}

		DPrintf("%d接收了%d(任期:%d)的AppendEntries, 成功返回(%d, %d)！", rf.me, args.LeaderId, reply.Term, reply.NextIndex, reply.NextTerm)
	} else { // the term missmatch
		reply.Success = false

		reply.NextTerm = prevLogTerm
		reply.NextIndex = rf.snapshotIndex
		for index := prevLogIndex - 1; index >= rf.snapshotIndex; index-- {
			if rf.logs[index-rf.snapshotIndex].Term != prevLogTerm {
				reply.NextIndex = index + 1
				break
			}
		}
		DPrintf("%d拒绝了%d(任期:%d)的AppendEntries, 因为在相同 prevLogIndex下 Term不匹配, 需要在下次heartbeat从(%d, %d)开始发送Log！", rf.me, args.LeaderId, reply.Term, reply.NextIndex, reply.NextTerm)
	}
	// ======= 2B code end here ======
	rf.persist()
	return
}

// leader send AppenEntries out periodically for heartbeats and log replication
func (rf *Raft) sendAppenEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppenEntries", args, reply)
	return ok
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	select {
	case <-rf.shutdownCh:
		DPrintf("[%d-%s]: peer %d is shutting down, reject install snapshot rpc request.\n",
			rf.me, rf.state, rf.me)
		return
	default:
	}
	DPrintf("[%d-%s]: rpc Install snapshot, from peer: %d, term: %d\n", rf.me, rf.state, args.LeaderID, args.Term)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.CurrentTerm = rf.currentTerm
	if args.Term < rf.currentTerm {
		DPrintf("[%d-%s]: rpc Install snapshot, args.term < rf.CurrentTerm (%d < %d)\n", rf.me, rf.state,
			args.Term, rf.currentTerm)
		return
	}
	rf.resetTimerCh <- struct{}{}

	if args.LastIncludedIndex <= rf.snapshotIndex {
		DPrintf("[%d-%s]: rpc Install snapshot, args.LastIncludedIndex <= rf.snapshotIndex (%d < %d)\n", rf.me, rf.state,
			args.LastIncludedIndex, rf.snapshotIndex)
		return
	}

	// snapshot contains all the logs
	if args.LastIncludedIndex >= rf.snapshotIndex+len(rf.logs)-1 {
		DPrintf("[%d-%s]: rpc snapshot, snapshot have all logs (%d >= %d + %d - 1).\n", rf.me, rf.state,
			args.LastIncludedIndex, rf.snapshotIndex, len(rf.logs))
		rf.snapshotIndex = args.LastIncludedIndex
		rf.snapshotTerm = args.LastIncludedTerm
		rf.commitIndex = rf.snapshotIndex
		rf.lastApplied = rf.snapshotIndex
		rf.logs = []LogEntry{{rf.snapshotTerm, nil}}
		rf.applyCh <- ApplyMsg{false, nil, rf.snapshotIndex, args.Snapshot}
		rf.persist()
		return
	}

	// snapshot contains part of the logs
	DPrintf("[%d-%s]: rpc snapshot, snapshot have part of logs (%d < %d + %d - 1).\n", rf.me, rf.state,
		args.LastIncludedIndex, rf.snapshotIndex, len(rf.logs))
	rf.logs = rf.logs[args.LastIncludedIndex-rf.snapshotIndex:]
	rf.logs[0].Term = args.LastIncludedTerm
	rf.snapshotIndex = args.LastIncludedIndex
	rf.snapshotTerm = args.LastIncludedTerm
	rf.commitIndex = rf.snapshotIndex
	rf.lastApplied = rf.snapshotIndex
	rf.applyCh <- ApplyMsg{false, nil, rf.snapshotIndex, args.Snapshot}
	rf.persist()
	return

}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool

	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = (rf.state == Leader)
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
	DPrintf("[%d-%s] persist its state at term %d", rf.me, rf.state, rf.currentTerm)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.snapshotIndex)
	e.Encode(rf.snapshotTerm)

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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []LogEntry
	var snapshotIndex int
	var snapshotTerm int

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil ||
		d.Decode(&snapshotIndex) != nil ||
		d.Decode(&snapshotTerm) != nil {
		// error...
		DPrintf("there happens some error in rf.readPersist")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
		rf.snapshotIndex = snapshotIndex
		rf.snapshotTerm = snapshotTerm
	}
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	DPrintf("[%d-%s] is killed @ term %d", rf.me, rf.state, rf.currentTerm)
	close(rf.shutdownCh)
}

// goroutine to start a new election
func (rf *Raft) election(term int) {

	rf.mu.Lock()

	if rf.state == Leader {
		// fmt.Printf("DEBUG: %d is leader in term %d, but issue election", rf.me, rf.currentTerm)
		rf.mu.Unlock()
		return
	}

	if term != rf.currentTerm {
		// fmt.Printf("DEBUG: the current term %d is larger than the term issue the election %d", rf.currentTerm, term)
		rf.mu.Unlock()
		return
	}

	rf.votedFor = rf.me
	rf.state = Candidate
	rf.currentTerm += 1

	rf.persist()

	lastLogIndex, lastLogTerm := rf.getLastLogState()
	voteArgs := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	rf.mu.Unlock()

	var voteNum = 1

	replyHandler := func(reply *RequestVoteReply, server int, issueTerm int) {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if rf.state == Candidate {

			if rf.currentTerm > issueTerm+1 {
				//fmt.Printf("DEBUG: 当前任期为%d, 大于发起选举时的任期(%d)了", rf.currentTerm, issueTerm)
				return
			}

			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.turnToFollower()
				rf.resetTimerCh <- struct{}{} //reset Timer
				rf.persist()
				return
			}

			if reply.VoteGranted {
				voteNum += 1
				//fmt.Printf("Raft server %d get the vote from %d(@term %d), the current vote number is %d \n", rf.me, server, reply.Term, voteNum)
				if voteNum > len(rf.peers)/2 {
					// init Leader state
					for server, _ := range rf.peers {
						rf.nextIndex[server] = len(rf.logs) + rf.snapshotIndex
						rf.matchIndex[server] = 0
					}
					rf.matchIndex[rf.me] = len(rf.logs) - 1 + rf.snapshotIndex

					DPrintf("%d 获得了%d个选票，成为了 leader， 在任期 %d\n", rf.me, voteNum, rf.currentTerm)
					//fmt.Printf("%d 获得了%d个选票，成为了 leader， 在任期 %d\n", rf.me, voteNum, rf.currentTerm)
					rf.state = Leader
					go rf.heartbeatDaemon()
					return
				}
			}
		}

	}

	for server, _ := range rf.peers {
		if server != rf.me {
			go func(server int) {
				var reply RequestVoteReply
				if rf.sendRequestVote(server, &voteArgs, &reply) {
					replyHandler(&reply, server, term)
				}
			}(server)
		}
	}
}

// The election daemon
func (rf *Raft) electionDaemon() {
	timeout := time.Millisecond * time.Duration(400+rand.Intn(400))
	electionTimer := time.NewTimer(timeout)
	for {
		select {
		case <-rf.shutdownCh:
			DPrintf("[%d-%s]: peer %d is shutting down electionDaemon.\n", rf.me, rf.state, rf.me)
			return
		case <-rf.resetTimerCh:
			if !electionTimer.Stop() {
				<-electionTimer.C
			}
			timeout = time.Millisecond * time.Duration(400+rand.Intn(400))
			electionTimer.Reset(timeout)
		case <-electionTimer.C:
			DPrintf("[%d-%s]: peer %d election timeout, issue election @ term %d\n", rf.me, rf.state, rf.me, rf.currentTerm)
			//fmt.Printf("[%d-%s]: peer %d election timeout, issue election @ term %d\n", rf.me, rf.state, rf.me, rf.currentTerm)
			go rf.election(rf.currentTerm)
			timeout = time.Millisecond * time.Duration(400+rand.Intn(400))
			electionTimer.Reset(timeout)
		}
	}
}

func (rf *Raft) updateCommitIndex() {
	// rf.mu.Lock()
	// defer rf.mu.Unlock()

	match := make([]int, len(rf.matchIndex))
	copy(match, rf.matchIndex)
	sort.Sort(sort.Reverse(sort.IntSlice(match)))
	// sort.Ints(match)

	target := match[len(match)/2]
	DPrintf("[%d-%s]: leader %d try to update commit index: %v @ term %d.\n",
		rf.me, rf.state, rf.me, rf.matchIndex, rf.currentTerm)

	if target > rf.commitIndex { // updateCommitIndex

		if rf.logs[target-rf.snapshotIndex].Term == rf.currentTerm {
			rf.commitIndex = target

			// signal possible update commit index
			go func() { rf.commitCond.Signal() }()

		}

	}
}

func (rf *Raft) heartbeatReplyHandler(server int, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.turnToFollower()
		rf.persist()
		rf.resetTimerCh <- struct{}{}
		DPrintf("Leader%d收到了来自%d的heartbeatReply,由于 Leader%任期落后，转为Follower", rf.me, server, rf.me)
		return
	}

	DPrintf("Leader%d收到了来自%d的heartbeatReply", rf.me, server)

	if reply.Success {
		rf.matchIndex[server] = reply.NextIndex - 1
		rf.nextIndex[server] = reply.NextIndex
		rf.updateCommitIndex() // try to update commitIndex
	} else {
		if reply.NextIndex <= rf.snapshotIndex && rf.snapshotIndex != 0 {
			rf.sendSnapshot(server)
			return
		}
		if rf.logs[reply.NextIndex-rf.snapshotIndex].Term == reply.Term {
			DPrintf("Log replicate 失败，返回的 matchIndex 能够和 matchTerm对应！！！！！！！")
			rf.nextIndex[server] = reply.NextIndex
		} else {
			DPrintf("Log replicate 失败，返回的 matchIndex 不能和 matchTerm对应-----------")
			rf.nextIndex[server] = max(reply.NextIndex-1, rf.snapshotIndex+1)
		}
	}
}

// goroutine to start a new election
func (rf *Raft) heartbeat(server int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	prevLogIndex := rf.nextIndex[server] - 1
	// if prevLogIndex < 0 || prevLogIndex >= len(rf.logs)+rf.snapshotIndex {
	// 	fmt.Printf("ERROR: Server[%d-%s] (term: %d) in heartbeat function to %d: prevLogIndex %d, len(rf.logs)=%d\n", rf.me, rf.state, rf.currentTerm, server, prevLogIndex, len(rf.logs))
	// } else {
	// 	fmt.Printf("Server [%d-%s] (term: %d) in heartbeat function to %d: prevLogIndex %d, len(rf.logs)=%d\n", rf.me, rf.state, rf.currentTerm, server, prevLogIndex, len(rf.logs))
	// }

	if prevLogIndex < rf.snapshotIndex && rf.snapshotIndex != 0 {
		rf.sendSnapshot(server)
		return
	}

	prevLogTerm := rf.logs[prevLogIndex-rf.snapshotIndex].Term

	heartbeatArgs := AppendEntriesArgs{
		Term:         rf.currentTerm, // leader's term
		LeaderId:     rf.me,          // so follower can redirect clients
		PrevLogIndex: prevLogIndex,   // index of log entry immediately preceding new ones
		PrevLogTerm:  prevLogTerm,    // term of prevLogIndex entry
		Entries:      nil,            // log entries to store(empty for heartbeats; may send more than one for efficiency)
		LeaderCommit: rf.commitIndex, // leader's commitIndex
	}

	if prevLogIndex < len(rf.logs)-1+rf.snapshotIndex {
		heartbeatArgs.Entries = append(heartbeatArgs.Entries, rf.logs[prevLogIndex+1-rf.snapshotIndex:]...)
	}

	go func(server int) {
		if _, isLeader := rf.GetState(); !isLeader {
			return
		}
		var reply AppendEntriesReply
		// DPrintf("[%d-%s]: consistency Check to peer %d.\n", rf.me, rf.state, server)
		if rf.sendAppenEntries(server, &heartbeatArgs, &reply) {
			rf.heartbeatReplyHandler(server, &reply)
		}
	}(server)

}

// the heartbeat daemon
func (rf *Raft) heartbeatDaemon() {
	DPrintf("%d start a heartbeat daemon @term %d", rf.me, rf.currentTerm)
	heartbeatTicker := time.NewTicker(100 * time.Millisecond)
	for {
		rf.resetTimerCh <- struct{}{}
		if _, isLeader := rf.GetState(); !isLeader {
			return
		}
		select {
		case <-rf.shutdownCh:
			return
		case <-heartbeatTicker.C:
			for server, _ := range rf.peers {
				if server != rf.me {
					go rf.heartbeat(server)
				}
			}
		default:
		}
	}
}

// should be called when hold the Lock
func (rf *Raft) sendSnapshot(server int) {
	var args = &InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LastIncludedIndex: rf.snapshotIndex,
		LastIncludedTerm:  rf.snapshotTerm,
		LeaderID:          rf.me,
		Snapshot:          rf.persister.ReadSnapshot(),
	}

	replyHandler := func(server int, reply *InstallSnapshotReply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.state == Leader {
			if reply.CurrentTerm > rf.currentTerm {
				rf.currentTerm = reply.CurrentTerm
				rf.turnToFollower()
				return
			}
			rf.matchIndex[server] = rf.snapshotIndex
			rf.nextIndex[server] = rf.snapshotIndex + 1
		}
	}

	go func() {
		var reply InstallSnapshotReply
		if rf.sendInstallSnapshot(server, args, &reply) {
			replyHandler(server, &reply)
		}
	}()
}

func (rf *Raft) applyLogEntriesDaemon() {
	for {
		rf.mu.Lock()
		for rf.lastApplied == rf.commitIndex {
			rf.commitCond.Wait()

			select {
			case <-rf.shutdownCh:
				rf.mu.Unlock()
				DPrintf("[%d-%s]: peer %d is shutting down apply log entry to client daemon.\n", rf.me, rf.state, rf.me)
				close(rf.applyCh)
				return
			default:
			}
		}

		lastApplied, commitIndex := rf.lastApplied, rf.commitIndex
		var logs []LogEntry
		if lastApplied < commitIndex {
			rf.lastApplied = rf.commitIndex
			logs = make([]LogEntry, commitIndex-lastApplied)
			copy(logs, rf.logs[lastApplied+1-rf.snapshotIndex:commitIndex+1-rf.snapshotIndex])
		}

		rf.mu.Unlock()

		for i := 0; i < commitIndex-lastApplied; i++ {
			reply := ApplyMsg{
				CommandValid: true,
				CommandIndex: lastApplied + i + 1,
				Command:      logs[i].Command,
			}
			DPrintf("[%d-%s]: peer %d apply %v to client.\n", rf.me, rf.state, rf.me, reply)
			rf.applyCh <- reply
		}
	}
}

func (rf *Raft) NewSnapshot(index int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.commitIndex < index || index <= rf.snapshotIndex {
		panic("NewSnapShot(): new.snapshotIndex <= old.snapshotIndex")
	}

	rf.logs = rf.logs[index-rf.snapshotIndex:]
	rf.snapshotIndex = index
	rf.snapshotTerm = rf.logs[0].Term

	// fmt.Printf("[%d-%s]: peer %d have new snapshot, %d @ %d.\n",
	// 	rf.me, rf.state, rf.me, rf.snapshotIndex, rf.snapshotTerm)
	rf.persist()
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
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

	select {
	case <-rf.shutdownCh:
		return index, term, false
	default:
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// if this server isn't the leader, returns false.
	if rf.state != Leader {
		return index, term, false
	}

	// otherwise start the agreement and return immediately.
	index = len(rf.logs) + rf.snapshotIndex
	term = rf.currentTerm
	logEntry := LogEntry{
		Term:    rf.currentTerm,
		Command: command,
	}
	rf.logs = append(rf.logs, logEntry)

	// only update leader itself
	rf.nextIndex[rf.me] += 1
	rf.matchIndex[rf.me] += 1

	// the Log replication is actually done by heartbeat(AppenEntries RPC)
	// so just return
	DPrintf("[%d-%s]: client add new entry (%d-%v)\n", rf.me, rf.state, index, command)
	rf.persist()

	return index, term, isLeader
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
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = Follower
	rf.commitCond = sync.NewCond(&rf.mu)

	rf.logs = make([]LogEntry, 1)
	rf.logs[0] = LogEntry{ // first index is 1
		Term:    0,
		Command: nil,
	}

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	// create a background goroutine that will kick off leader election periodically
	// by sending out RequestVote RPCs when it hasn't heard from another peer for a while.
	rf.resetTimerCh = make(chan struct{})
	rf.shutdownCh = make(chan struct{}) // shutdown raft gracefully

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.commitIndex = rf.snapshotIndex
	rf.lastApplied = rf.snapshotIndex

	go rf.electionDaemon()
	go rf.applyLogEntriesDaemon()

	return rf
}
