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
	//	"bytes"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"MIT6824/labgob"
	"MIT6824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

const (
	FOLLOWER  = 0
	CANDIDATE = 1
	LEADER    = 2
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent state on all servers
	currentTerm int
	votedFor    int // reset to -1 when term changes
	log         []LogEntry
	// volatile state on all servers
	commitIndex int
	lastApplied int
	// volatile state on leaders
	nextIndex  []int
	matchIndex []int
	agreeNum   []int // agree number of every log
	state      int   // 0: follower, 1: candidate, 2: leader

	replicatorCond []*sync.Cond  // used to signal replicator goroutine to batch replicating entries
	applyCond      *sync.Cond    // used to signal applier goroutine to apply entries
	applyCh        chan ApplyMsg // channel to send ApplyMsg to service

	heartBeatInterval  time.Duration
	nextReElectionTime time.Time
}

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == LEADER
	// isleader = rf.isLeader
	rf.mu.Unlock()
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
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
	XTerm   int
	XIndex  int
	XLen    int
}

/******************************************************************************************
* RPC handler
 *******************************************************************************************/

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		Debug(dTerm, "S%d become T%d\n", rf.me, args.Term)
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.logCheck(args) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.resetTimer()
		rf.state = FOLLOWER
		Debug(dVote, "S%d Granting Vote to S%d at T%d\n", rf.me, args.CandidateId, rf.currentTerm)
		return
	}
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	Debug(dLog, "S%d receive append entries from S%d at T%d\n", rf.me, args.LeaderId, rf.currentTerm)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 1
	if args.Term < rf.currentTerm {
		Debug(dWarn, "S%d (T%d)receive append entries from S%d (T%d)but refuse\n", rf.me, rf.currentTerm, args.LeaderId, args.Term)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	if args.Term > rf.currentTerm {
		Debug(dTerm, "S%d become T%d\n", rf.me, args.Term)
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = FOLLOWER
	}
	rf.resetTimer()

	// 2
	if len(rf.log) < args.PrevLogIndex+1 || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Term = rf.currentTerm
		reply.Success = false

		if len(rf.log) < args.PrevLogIndex+1 {
			reply.XLen = len(rf.log)
		} else {
			reply.XLen = -1
			reply.XTerm = rf.log[args.PrevLogIndex].Term
			reply.XIndex = rf.getFirstIndexOfTerm(reply.XTerm, args.PrevLogIndex)
		}
		return
	}
	// 3 4
	rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)

	// 5
	if min(args.LeaderCommit, rf.getLastLog().Index) > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.getLastLog().Index)
		Debug(dCommit, "S%d commit index to %d at T%d\n", rf.me, rf.commitIndex, rf.currentTerm)
		rf.applyCond.Signal()
	}

	reply.Term = rf.currentTerm
	reply.Success = true
}

/*******************************************************************************************
*  helper function
 *******************************************************************************************/
func (rf *Raft) getFirstIndexOfTerm(term int, prevLogIndex int) int {
	for i := prevLogIndex - 1; i >= 0; i-- {
		if rf.log[i].Term != term {
			return i + 1
		}
	}
	return prevLogIndex
}

func (rf *Raft) logCheck(args *RequestVoteArgs) bool {
	if rf.log[len(rf.log)-1].Term < args.LastLogTerm {
		return true
	}
	if rf.log[len(rf.log)-1].Term == args.LastLogTerm && len(rf.log)-1 <= args.LastLogIndex {
		return true
	}
	return false
}

// must get lock before calling this function
func (rf *Raft) resetTimer() {
	Debug(dTimer, "S%d reset timer\n", rf.me)
	ms := 250 + (rand.Int63() % 150)
	rf.nextReElectionTime = time.Now().Add(time.Duration(ms) * time.Millisecond)
}

func (rf *Raft) hasTerm(term int, index int) (bool, int) {
	for i := index - 1; i >= 0; i-- {
		if rf.log[i].Term == term {
			return true, i
		}
	}
	return false, -1
}

/*******************************************************************************************
* RPC send
 ******************************************************************************************/

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	Debug(dVote, "S%d send request vote to S%d at T%d\n", rf.me, server, args.Term)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	Debug(dLeader, "S%d send append entries to S%d at T%d\n", rf.me, server, args.Term)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

/*
* Election
 */
func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.state = CANDIDATE
	resCh := make(chan RequestVoteReply) // channel to receive vote result
	rf.resetTimer()

	var wg sync.WaitGroup
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			wg.Add(1)
			go func(i int, currentTerm int, LastLogIndex int, LastLogTerm int) {
				defer wg.Done()
				reply := RequestVoteReply{}
				if rf.sendRequestVote(i, &RequestVoteArgs{currentTerm, rf.me, LastLogIndex, LastLogTerm}, &reply) {
					resCh <- reply
				}
			}(i, rf.currentTerm, len(rf.log)-1, rf.log[len(rf.log)-1].Term)
		}
	}
	rf.mu.Unlock()

	// when last go routine finished, close the channel
	go func() {
		wg.Wait()
		close(resCh)
	}()

	voteCnt := 1 // vote for itself
	for reply := range resCh {
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.state = FOLLOWER
			rf.mu.Unlock()
			return
		}
		if reply.VoteGranted {
			voteCnt++
			if voteCnt > len(rf.peers)/2 {
				rf.state = LEADER
				rf.agreeNum = make([]int, len(rf.peers))
				rf.nextIndex = make([]int, len(rf.peers))
				rf.matchIndex = make([]int, len(rf.peers))
				for i := 0; i < len(rf.peers); i++ {
					rf.nextIndex[i] = len(rf.log)
					rf.matchIndex[i] = 0
				}

				Debug(dLeader, "S%d become leader at T%d\n", rf.me, rf.currentTerm)
				rf.mu.Unlock()
				return
			}
		}
		rf.mu.Unlock()
	}

}

func (rf *Raft) leaderBroadCast(isHeartBeat bool) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			if isHeartBeat {
				go rf.replicateOneRound(i)
			} else {
				rf.replicatorCond[i].Signal()
			}

		}
	}
}

func (rf *Raft) replicateOneRound(peer int) {
	rf.mu.RLock()
	if rf.state != LEADER {
		rf.mu.RUnlock()
		return
	}
	prevLogIndex := rf.nextIndex[peer] - 1
	if prevLogIndex < rf.getFirstLog().Index {
		Debug(dWarn, "prevLogIndex %d,first %d\n", prevLogIndex, rf.getFirstLog().Index)
		// only snapshot can catch up
		//(uncomment after implement snapshot)
		// request := rf.genInstallSnapshotRequest()
		rf.mu.RUnlock()
		// response := new(InstallSnapshotResponse)
		// if rf.sendInstallSnapshot(peer, request, response) {
		// 	rf.mu.Lock()
		// 	rf.handleInstallSnapshotResponse(peer, request, response)
		// 	rf.mu.Unlock()
		// }
	} else {
		// just entries can catch up
		request := rf.genAppendEntriesRequest(prevLogIndex)
		rf.mu.RUnlock()
		response := new(AppendEntriesReply)
		if rf.sendAppendEntries(peer, request, response) {
			rf.mu.Lock()
			rf.handleAppendEntriesResponse(peer, request, response)
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) handleAppendEntriesResponse(peer int, request *AppendEntriesArgs, response *AppendEntriesReply) {
	if response.Term > rf.currentTerm {
		Debug(dTerm, "S%d become follower at T%d\n", rf.me, response.Term)
		rf.currentTerm = response.Term
		rf.votedFor = -1
		rf.state = FOLLOWER
		return
	}
	if len(request.Entries) == 0 {
		return
	}
	if response.Success {
		rf.nextIndex[peer] = request.PrevLogIndex + len(request.Entries) + 1
		rf.matchIndex[peer] = rf.nextIndex[peer] - 1
		rf.updateCommitIndex()
	} else {
		// rf.nextIndex[peer]--

		res, index := rf.hasTerm(response.XTerm, request.PrevLogIndex)
		if response.XLen != -1 {
			rf.nextIndex[peer] = response.XLen
		} else {
			if res {
				rf.nextIndex[peer] = index
			} else {
				rf.nextIndex[peer] = response.XIndex
			}
		}

	}
}

func (rf *Raft) updateCommitIndex() {
	sortMatchIndex := make([]int, 0)
	sortMatchIndex = append(sortMatchIndex, rf.getLastLog().Index)
	for i := 0; i < len(rf.matchIndex); i++ {
		if i != rf.me {
			sortMatchIndex = append(sortMatchIndex, rf.matchIndex[i])
		}
	}

	sort.Ints(sortMatchIndex)
	midIndex := len(rf.peers) / 2
	// if len(rf.peers)%2 == 0 { //理论上rf.peers为奇数长度
	// 	midIndex--
	// }
	newCommitIndex := sortMatchIndex[midIndex]

	if newCommitIndex > rf.commitIndex && rf.log[newCommitIndex].Term == rf.currentTerm {
		Debug(dCommit, "S%d commit index to %d at T%d\n", rf.me, newCommitIndex, rf.currentTerm)
		rf.commitIndex = newCommitIndex
		rf.applyCond.Signal()
	}
}

func (rf *Raft) genAppendEntriesRequest(prevLogIndex int) *AppendEntriesArgs {
	request := &AppendEntriesArgs{}
	request.Term = rf.currentTerm
	request.LeaderId = rf.me
	request.PrevLogIndex = prevLogIndex
	request.PrevLogTerm = rf.log[prevLogIndex].Term
	if prevLogIndex+1 < len(rf.log) {
		request.Entries = rf.log[prevLogIndex+1:]
	} else {
		request.Entries = []LogEntry{}
	}
	request.LeaderCommit = rf.commitIndex
	return request
}

func (rf *Raft) replicator(peer int) {
	rf.replicatorCond[peer].L.Lock()
	defer rf.replicatorCond[peer].L.Unlock()
	for !rf.killed() {
		// if there is no need to replicate entries for this peer, just release CPU and wait other goroutine's signal if service adds new Command
		// if this peer needs replicating entries, this goroutine will call replicateOneRound(peer) multiple times until this peer catches up, and then wait
		for !rf.needReplicating(peer) {
			rf.replicatorCond[peer].Wait()
		}
		// maybe a pipeline mechanism is better to trade-off the memory usage and catch up time
		rf.replicateOneRound(peer)
	}
}

func (rf *Raft) needReplicating(peer int) bool {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.state == LEADER && rf.matchIndex[peer] < rf.getLastLog().Index
}

func (rf *Raft) getLastLog() LogEntry {
	return rf.log[len(rf.log)-1]
}

func (rf *Raft) getFirstLog() LogEntry {
	return rf.log[0]
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := len(rf.log)
	term := rf.currentTerm
	isLeader := rf.state == LEADER

	if isLeader {
		Debug(dClient, "S%d receive command %v,index %d at T%d\n", rf.me, command, index, rf.currentTerm)
		rf.log = append(rf.log, LogEntry{term, index, command})
		go rf.leaderBroadCast(false)
	}

	// Your code here (2B).

	return index, term, isLeader
}

func (rf *Raft) applier() {
	rf.applyCond.L.Lock()
	defer rf.applyCond.L.Unlock()
	for !rf.killed() {
		for !rf.canApply() {
			rf.applyCond.Wait()
		}

		rf.mu.Lock()
		for rf.commitIndex > rf.lastApplied {
			Debug(dClient, "S%d apply %d at T%d\n", rf.me, rf.lastApplied+1, rf.currentTerm)
			msg := ApplyMsg{CommandValid: true, Command: rf.log[rf.lastApplied+1].Command, CommandIndex: rf.lastApplied + 1}
			rf.applyCh <- msg
			rf.lastApplied++
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) canApply() bool {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.commitIndex > rf.lastApplied
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		time.Sleep(rf.heartBeatInterval)
		rf.mu.Lock()

		if rf.state == LEADER {
			Debug(dTimer, "S%d HeartBeat\n", rf.me)
			go rf.leaderBroadCast(true)
		} else if rf.state != LEADER && time.Now().After(rf.nextReElectionTime) {
			Debug(dTimer, "S%d Reelection\n", rf.me)
			if rf.state == FOLLOWER {
				rf.state = CANDIDATE
			}
			go rf.startElection()
		}

		rf.mu.Unlock()

	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:          peers,
		me:             me,
		persister:      persister,
		applyCh:        applyCh,
		replicatorCond: make([]*sync.Cond, len(peers)),
		applyCond:      sync.NewCond(&sync.Mutex{}),
	}

	// Your initialization code here (2A, 2B, 2C).
	rf.state = FOLLOWER
	rf.votedFor = -1
	rf.log = append(rf.log, LogEntry{0, 0, nil})
	rf.heartBeatInterval = 100 * time.Millisecond

	for i := 0; i < len(peers); i++ {
		// rf.matchIndex[i], rf.nextIndex[i] = 0, lastLog.Index+1
		if i != rf.me {
			rf.replicatorCond[i] = sync.NewCond(&sync.Mutex{})
			// start replicator goroutine to replicate entries in batch
			go rf.replicator(i)
		}
	}
	rf.resetTimer()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier()

	return rf
}
