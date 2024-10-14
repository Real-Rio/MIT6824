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
	"bytes"
	"math/rand"
	"slices"

	// "sort"
	"sync"
	"sync/atomic"
	"time"

	//	"MIT6824/labgob"
	"MIT6824/labgob"
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
	CommandTerm  int
	// IfLeader     bool
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
	currentTerm      int
	votedFor         int // reset to -1 when term changes
	log              []LogEntry
	lastIncludeIndex int // index of the last included log in snapshot
	lastIncludeTerm  int // term of the last included log in snapshot
	snapshot         []byte
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

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == LEADER
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
	w := new(bytes.Buffer)
	// s := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	// s2 := labgob.NewEncoder(s)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludeIndex)
	e.Encode(rf.lastIncludeTerm)
	// s2.Encode(rf.snapshot)

	raftstate := w.Bytes()
	// snapstate := s.Bytes()
	rf.persister.Save(raftstate, rf.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte, snapshotData []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	Debug(dPersist, "S%d read persist\n", rf.me)
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	// s :=bytes.NewBuffer(snapshotData)
	// d2 := labgob.NewDecoder(s)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	var lastIncludeIndex int
	var lastIncludeTerm int
	// var snapshot []byte

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil || d.Decode(&log) != nil || d.Decode(&lastIncludeIndex) != nil || d.Decode(&lastIncludeTerm) != nil {
		Debug(dWarn, "S%d read persist error\n", rf.me)
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.lastIncludeIndex = lastIncludeIndex
		rf.lastIncludeTerm = lastIncludeTerm
	}
	if len(snapshotData) > 0 && rf.lastIncludeIndex > 0 {
		rf.snapshot = snapshotData
		rf.lastApplied = rf.lastIncludeIndex
		rf.applyCond.Signal()
		// go rf.applySnapshot(lastIncludeIndex, lastIncludeTerm, snapshotData)
	}

}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	Debug(dSnap, "S%d receive snapshot(client) at index %d,prevIndex is %d\n", rf.me, index, rf.lastIncludeIndex)
	defer rf.mu.Unlock()
	// index <= snapshot's index
	if index <= rf.lastIncludeIndex {
		return
	}

	rf.lastIncludeTerm = rf.getLogByIndex(index).Term
	newLog := make([]LogEntry, len(rf.log[index-rf.lastIncludeIndex:]))
	copy(newLog, rf.log[index-rf.lastIncludeIndex:])
	rf.log = newLog
	// rf.log = rf.log[index-rf.beginIndex:]
	// rf.log[0].Command = nil
	rf.snapshot = snapshot
	rf.lastIncludeIndex = index
	Debug(dSnap, "S%d snapshot done,lastIncludeIndex is %d\n", rf.me, rf.lastIncludeIndex)
	rf.persist()
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
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
	// Offset            int
	// Done              bool
}

type InstallSnapshotReply struct {
	Term int
}

/******************************************************************************************
* RPC handler
 *******************************************************************************************/

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if args.Term > rf.currentTerm {
		Debug(dTerm, "S%d become T%d receive higher RV\n", rf.me, args.Term)
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.currentTerm == args.Term && rf.logCheck(args) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		// rf.persist()
		rf.resetTimer()
		rf.state = FOLLOWER
		Debug(dVote, "S%d Granting Vote to S%d at T%d\n", rf.me, args.CandidateId, rf.currentTerm)
		return
	}
	Debug(dVote, "S%d refuse vote to S%d at T%d\n", rf.me, args.CandidateId, rf.currentTerm)
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	Debug(dLog, "S%d receive append entries from S%d at T%d,len(%d) curlen(%d)\n", rf.me, args.LeaderId, rf.currentTerm, len(args.Entries), len(rf.log))
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
		rf.persist()
	}
	rf.state = FOLLOWER
	rf.resetTimer()

	// 2 return false because of mismatch
	if rf.getLastLog().Index < args.PrevLogIndex || rf.getFirstLog().Index > args.PrevLogIndex {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.ConflictIndex = rf.getLastLog().Index + 1
		reply.ConflictTerm = -1
		Debug(dLog, "S%d refuse append entries from S%d,mismatch1\n", rf.me, args.LeaderId)
		return
	} else if rf.getLogByIndex(args.PrevLogIndex).Term != args.PrevLogTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.ConflictTerm = rf.getLogByIndex(args.PrevLogIndex).Term
		// jump over all the log entries with the conflict term
		for i := 0; i < len(rf.log); i++ {
			if rf.log[i].Term == reply.ConflictTerm {
				reply.ConflictIndex = i + rf.lastIncludeIndex
				break
			}
		}
		newLog := make([]LogEntry, len(rf.log[:args.PrevLogIndex-rf.lastIncludeIndex]))
		copy(newLog, rf.log[:args.PrevLogIndex-rf.lastIncludeIndex])
		rf.log = newLog
		if len(rf.log) == 0 {
			rf.log = append(rf.log, LogEntry{rf.lastIncludeTerm, rf.lastIncludeIndex, nil})
		}
		Debug(dLog, "S%d refuse append entries from S%d,mismatch2\n", rf.me, args.LeaderId)
		return
	}
	// 3 4
	if rf.getLastLog().Index > args.PrevLogIndex+len(args.Entries) {
		for i := 1; i <= len(args.Entries); i++ {
			if rf.getLogByIndex(args.PrevLogIndex+i).Term != args.Entries[i-1].Term {
				rf.log = append(rf.log[:args.PrevLogIndex+1-rf.lastIncludeIndex], args.Entries...)
				break
			}
		}
	} else {
		rf.log = append(rf.log[:args.PrevLogIndex+1-rf.lastIncludeIndex], args.Entries...)
	}
	rf.persist()

	// 5
	boundry := min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
	if min(boundry, rf.getLastLog().Index) > rf.commitIndex {
		rf.commitIndex = min(boundry, rf.getLastLog().Index)
		Debug(dCommit, "S%d commit index to %d at T%d\n", rf.me, rf.commitIndex, rf.currentTerm)
		rf.applyCond.Signal()
	}
	Debug(dLog, "S%d accept append entries(%d) from S%d at T%d(successfully)\n", rf.me, len(args.Entries), args.LeaderId, rf.currentTerm)
	reply.Term = rf.currentTerm
	reply.Success = true
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	Debug(dSnap, "S%d receive snapshot from S%d at T%d lastIndex %d\n", rf.me, args.LeaderId, rf.currentTerm, args.LastIncludedIndex)
	reply.Term = rf.currentTerm

	// 1
	if args.Term < rf.currentTerm {
		Debug(dSnap, "S%d (T%d)receive snapshot from S%d (T%d)but refuse\n", rf.me, rf.currentTerm, args.LeaderId, args.Term)
		return
	}

	if args.Term > rf.currentTerm {
		Debug(dTerm, "S%d become T%d\n", rf.me, args.Term)
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}
	rf.state = FOLLOWER
	rf.resetTimer()

	if args.LastIncludedIndex <= rf.commitIndex || args.LastIncludedIndex <= rf.lastIncludeIndex {
		Debug(dSnap, "S%d refuse snapshot from S%d,lastIncludedIndex %d,commitIndex %d,lastIncludeIndex %d\n", rf.me, args.LeaderId, args.LastIncludedIndex, rf.commitIndex, rf.lastIncludeIndex)
		return
	}

	if args.LastIncludedIndex <= rf.getLastLog().Index {
		newLog := make([]LogEntry, len(rf.log[args.LastIncludedIndex-rf.lastIncludeIndex:]))
		copy(newLog, rf.log[args.LastIncludedIndex-rf.lastIncludeIndex:])
		rf.log = newLog
		// rf.log[0].Command = nil
	} else {
		rf.log = []LogEntry{{args.LastIncludedTerm, args.LastIncludedIndex, nil}}
	}
	Debug(dWarn, "S%d log length is %d\n", rf.me, len(rf.log))
	rf.lastIncludeIndex = args.LastIncludedIndex
	rf.lastIncludeTerm = args.LastIncludedTerm
	rf.snapshot = args.Data
	rf.persist()

	rf.applyCond.Signal()
	// go rf.applySnapshot(args.LastIncludedIndex, args.LastIncludedTerm, args.Data)
}

/*******************************************************************************************
*  helper function
 *******************************************************************************************/
func (rf *Raft) logCheck(args *RequestVoteArgs) bool {
	if rf.getLastLog().Term < args.LastLogTerm {
		return true
	}
	if rf.getLastLog().Term == args.LastLogTerm && rf.getLastLog().Index <= args.LastLogIndex {
		return true
	}
	Debug(dVote, "S%d refuse vote to S%d at T%d,lockCheck failed\n", rf.me, args.CandidateId, rf.currentTerm)
	return false
}

// must get lock before calling this function
func (rf *Raft) resetTimer() {
	Debug(dTimer, "S%d reset timer\n", rf.me)
	ms := 200 + (rand.Int63() % 250)
	rf.nextReElectionTime = time.Now().Add(time.Duration(ms) * time.Millisecond)
}

func (rf *Raft) getLogByIndex(index int) LogEntry {
	// Debug(dLog2, "S%d get log by index %d,beginIndex %d\n", rf.me, index, rf.lastIncludeIndex)
	return rf.log[index-rf.lastIncludeIndex]
}

// 去掉快照后，第一条 log
func (rf *Raft) getFirstLog() LogEntry {
	if len(rf.log) == 0 {
		Debug(dWarn, "S%d log length is 0\n", rf.me)
	}
	return rf.log[0]
}

// 最后一条 log
func (rf *Raft) getLastLog() LogEntry {
	if len(rf.log) == 0 {
		Debug(dWarn, "S%d log length is 0\n", rf.me)
	}

	return rf.log[len(rf.log)-1]
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
	if len(args.Entries) > 0 {
		Debug(dLeader, "S%d send append entries to S%d at T%d from index %d(%d)\n", rf.me, server, args.Term, args.Entries[0].Index, len(args.Entries))

	} else {
		Debug(dLeader, "S%d send append entries to S%d at T%d\n", rf.me, server, args.Term)

	}
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	Debug(dSnap, "S%d send install snapshot to S%d at T%d lastIndex %d\n", rf.me, server, args.Term, args.LastIncludedIndex)
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// func (rf *Raft) applySnapshot(lastIncludeIndex int, lastIncludeTerm int, snapshot []byte) {
// 	rf.mu.Lock()
// 	Debug(dSnap, "S%d apply snapshot,prevIndex is %d\n", rf.me, lastIncludeIndex)
// 	rf.lastApplied = lastIncludeIndex
// 	rf.commitIndex = lastIncludeIndex
// 	rf.mu.Unlock()
// 	rf.applyCh <- ApplyMsg{
// 		SnapshotValid: true,
// 		Snapshot:      snapshot,
// 		// SnapshotTerm:  lastIncludeTerm,
// 		SnapshotIndex: lastIncludeIndex,
// 	}
// }

/*
* Election
 */
func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.state = CANDIDATE
	rf.resetTimer()
	rf.persist()
	resCh := make(chan RequestVoteReply) // channel to receive vote result

	var wg sync.WaitGroup
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			wg.Add(1)
			go func(i int, currentTerm int, LastLogIndex int, LastLogTerm int) {
				defer wg.Done()
				reply := RequestVoteReply{}
				if rf.sendRequestVote(i, &RequestVoteArgs{currentTerm, rf.me, LastLogIndex, LastLogTerm}, &reply) {
					resCh <- reply
				} else {
					Debug(dWarn, "S%d send request vote to S%d out of time\n", rf.me, i)
				}
			}(i, rf.currentTerm, rf.getLastLog().Index, rf.getLastLog().Term)
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
		Debug(dLog, "S%d receive vote reply\n", rf.me)
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.state = FOLLOWER
			rf.resetTimer()
			rf.persist()
			rf.mu.Unlock()
			// return
			continue
		}
		if rf.state != CANDIDATE {
			rf.mu.Unlock()
			return
		}
		if reply.VoteGranted && rf.currentTerm == reply.Term {
			voteCnt++
			if voteCnt > len(rf.peers)/2 {
				rf.state = LEADER
				rf.agreeNum = make([]int, len(rf.peers))
				rf.nextIndex = make([]int, len(rf.peers))
				rf.matchIndex = make([]int, len(rf.peers))
				for i := 0; i < len(rf.peers); i++ {
					rf.nextIndex[i] = rf.getLastLog().Index + 1
					rf.matchIndex[i] = -1
				}
				// no-op
				// rf.log = append(rf.log, LogEntry{rf.currentTerm, len(rf.log), nil})
				// rf.persist()
				// go rf.leaderBroadCast(false)
				go rf.leaderBroadCast(true)
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

	if rf.state != LEADER {
		return
	}

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go rf.replicateOneRound(i)
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
		// Debug(dWarn, "S%d prevLogIndex %d,first %d\n", rf.me, prevLogIndex, rf.getFirstLog().Index)
		// only snapshot can catch up
		request := rf.genInstallSnapshotRequest()
		rf.mu.RUnlock()
		response := new(InstallSnapshotReply)
		if rf.sendInstallSnapshot(peer, request, response) {
			rf.mu.Lock()
			rf.handleInstallSnapshotResponse(peer, request, response)
			rf.mu.Unlock()
		}
	} else {
		// just entries can catch up
		request := rf.genAppendEntriesRequest(prevLogIndex)
		rf.mu.RUnlock()
		response := new(AppendEntriesReply)
		if rf.sendAppendEntries(peer, request, response) {
			Debug(dLog, "S%d send append entries to S%d receive reply\n", rf.me, peer)
			rf.mu.Lock()
			rf.handleAppendEntriesResponse(peer, request, response)
			rf.mu.Unlock()
		} else {
			Debug(dWarn, "S%d send append entries to S%d receive no reply\n", rf.me, peer)
		}
	}
}

func (rf *Raft) handleAppendEntriesResponse(peer int, request *AppendEntriesArgs, response *AppendEntriesReply) {
	if response.Term > rf.currentTerm {
		Debug(dTerm, "S%d become follower at T%d\n", rf.me, response.Term)
		rf.currentTerm = response.Term
		rf.votedFor = -1
		rf.state = FOLLOWER
		rf.persist()
		return
	}
	if response.Term < rf.currentTerm || rf.state != LEADER {
		return
	}

	if response.Success {
		Debug(dLeader, "S%d receive reply successfully from S%d at T%d\n", rf.me, peer, rf.currentTerm)
		rf.nextIndex[peer] = request.PrevLogIndex + len(request.Entries) + 1
		rf.matchIndex[peer] = rf.nextIndex[peer] - 1
		rf.updateCommitIndex()
	} else {
		Debug(dLeader, "S%d receive reply fail from S%d at T%d\n", rf.me, peer, rf.currentTerm)
		// rf.nextIndex[peer]--

		newNextIdx := response.ConflictIndex
		if response.ConflictTerm != -1 {
			for i := 0; i < len(rf.log); i++ {
				if rf.log[i].Term != response.ConflictTerm {
					continue
				}
				for i < len(rf.log) && rf.log[i].Term == response.ConflictTerm {
					i++
				}
				newNextIdx = i
			}
		}
		rf.nextIndex[peer] = newNextIdx
		// Debug(dLeader, "S%d nextIndex[%d] to %d of T%d\n", rf.me, peer, newNextIdx, rf.getLogByIndex(newNextIdx-1).Term)
	}
}

func (rf *Raft) handleInstallSnapshotResponse(peer int, requeset *InstallSnapshotArgs, response *InstallSnapshotReply) {
	if response.Term > rf.currentTerm {
		Debug(dTerm, "S%d become follower at T%d\n", rf.me, response.Term)
		rf.currentTerm = response.Term
		rf.votedFor = -1
		rf.state = FOLLOWER
		rf.persist()
		return
	}
	if response.Term < rf.currentTerm || rf.state != LEADER {
		return
	}
	rf.nextIndex[peer] = requeset.LastIncludedIndex + 1
	rf.matchIndex[peer] = rf.nextIndex[peer] - 1

}

func (rf *Raft) updateCommitIndex() {
	sortMatchIndex := make([]int, 0)
	if len(rf.log) != 0 {
		sortMatchIndex = append(sortMatchIndex, rf.getLastLog().Index)
	} else {
		sortMatchIndex = append(sortMatchIndex, rf.lastIncludeIndex-1)
	}
	for i := 0; i < len(rf.matchIndex); i++ {
		if i != rf.me {
			sortMatchIndex = append(sortMatchIndex, rf.matchIndex[i])
		}
	}

	// sort.Ints(sortMatchIndex)
	slices.Sort(sortMatchIndex)
	midIndex := len(rf.peers) / 2
	if len(rf.peers)%2 == 0 { //理论上rf.peers为奇数长度
		midIndex--
	}
	newCommitIndex := sortMatchIndex[midIndex]

	if newCommitIndex > rf.commitIndex && rf.getLogByIndex(newCommitIndex).Term == rf.currentTerm {
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
	request.PrevLogTerm = rf.getLogByIndex(prevLogIndex).Term
	if prevLogIndex < rf.getLastLog().Index && prevLogIndex >= rf.getFirstLog().Index {
		// Debug(dWarn, "S%d prevLogIndex %d,first %d,begin %d\n", rf.me, prevLogIndex, rf.getFirstLog().Index, rf.lastIncludeIndex)
		request.Entries = make([]LogEntry, len(rf.log[prevLogIndex+1-rf.lastIncludeIndex:]))
		copy(request.Entries, rf.log[prevLogIndex+1-rf.lastIncludeIndex:])
		// request.Entries = rf.log[prevLogIndex+1-rf.beginIndex:]
	} else {
		request.Entries = []LogEntry{}
	}
	request.LeaderCommit = rf.commitIndex
	return request
}

func (rf *Raft) genInstallSnapshotRequest() *InstallSnapshotArgs {
	request := &InstallSnapshotArgs{}
	request.Term = rf.currentTerm
	request.LeaderId = rf.me
	request.LastIncludedIndex = rf.lastIncludeIndex
	request.LastIncludedTerm = rf.lastIncludeTerm
	request.Data = rf.snapshot
	return request
}

func (rf *Raft) needReplicating(peer int) bool {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.state == LEADER && rf.matchIndex[peer] < rf.getLastLog().Index
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
	index := rf.getLastLog().Index + 1
	term := rf.currentTerm
	isLeader := rf.state == LEADER

	if isLeader {
		Debug(dClient, "S%d receive command %v,index %d lastIncludeIndex %d(log len%d) at T%d\n", rf.me, command, index, rf.lastIncludeIndex, len(rf.log), rf.currentTerm)
		rf.log = append(rf.log, LogEntry{term, index, command})
		rf.persist()
		go rf.leaderBroadCast(false)
	}

	// Your code here (2B).

	return index, term, isLeader
}

func (rf *Raft) applier() {
	rf.applyCond.L.Lock()
	defer rf.applyCond.L.Unlock()
	for !rf.Killed() {
		for !rf.canApply() {
			rf.applyCond.Wait()
		}

		rf.mu.Lock()
		// 优先判断有没有 snapshot 要 apply
		if len(rf.snapshot) > 0 && rf.lastIncludeIndex > rf.lastApplied {
			Debug(dSnap, "S%d apply snapshot at T%d\n", rf.me, rf.currentTerm)
			rf.lastApplied = rf.lastIncludeIndex
			if rf.commitIndex < rf.lastIncludeIndex {
				rf.commitIndex = rf.lastIncludeIndex
			}
			rf.mu.Unlock()

			rf.applyCh <- ApplyMsg{
				SnapshotValid: true,
				Snapshot:      rf.snapshot,
				// SnapshotTerm:  lastIncludeTerm,
				SnapshotIndex: rf.lastIncludeIndex,
			}
			rf.mu.Lock()
		}

		for rf.commitIndex > rf.lastApplied && rf.lastApplied+1 > rf.lastIncludeIndex {
			Debug(dClient, "S%d apply %d at T%d\n", rf.me, rf.lastApplied+1, rf.currentTerm)
			msg := ApplyMsg{CommandValid: true, Command: rf.getLogByIndex(rf.lastApplied + 1).Command, CommandIndex: rf.lastApplied + 1, CommandTerm: rf.getLogByIndex(rf.lastApplied + 1).Term}
			if msg.Command == nil {
				Debug(dWarn, "S%d apply nil command index is %d at T%d\n", rf.me, msg.CommandIndex, rf.currentTerm)
				// msg.CommandValid = false
			}
			rf.lastApplied++
			rf.mu.Unlock()

			rf.applyCh <- msg
			Debug(dClient, "S%d current lastApplied is %d\n", rf.me, rf.lastApplied)
			rf.mu.Lock()
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

func (rf *Raft) Killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for !rf.Killed() {
		time.Sleep(rf.heartBeatInterval)
		rf.mu.RLock()

		if rf.state == LEADER {
			Debug(dTimer, "S%d HeartBeat\n", rf.me)
			go rf.leaderBroadCast(true)
		} else if rf.state != LEADER && time.Now().After(rf.nextReElectionTime) {
			Debug(dTimer, "S%d Reelection\n", rf.me)
			go rf.startElection()
		}

		rf.mu.RUnlock()

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
		peers:             peers,
		me:                me,
		persister:         persister,
		applyCh:           applyCh,
		replicatorCond:    make([]*sync.Cond, len(peers)),
		applyCond:         sync.NewCond(&sync.Mutex{}),
		state:             FOLLOWER,
		votedFor:          -1,
		log:               []LogEntry{{0, 0, nil}},
		heartBeatInterval: 100 * time.Millisecond,
		snapshot:          nil,
	}

	// Your initialization code here (2A, 2B, 2C).
	rf.readPersist(persister.ReadRaftState(), persister.ReadSnapshot())

	rf.resetTimer()

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier()

	return rf
}
