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
	"6.824/utils"
	"time"

	//	"bytes"
	"sync"
	"sync/atomic"
	//	"6.824/labgob"
	"6.824/labrpc"
)

type State string

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan ApplyMsg
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int
	log         []Entry

	// volatile state on all servers
	commitIndex int
	lastApplied int

	// volatile state on leader
	nextIndex  []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	state         State
	electionTime  time.Time
	heartbeatTime time.Time
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
	isleader = rf.state == leaderState
	return term, isleader
}

func (rf *Raft) sendRequestVote(server int, request *RequestVoteArgs, response *RequestVoteReply) bool {
	utils.Debug(utils.DInfo, "S%d send RequestVote request to %d {%+v}", rf.me, server, request)
	ok := rf.peers[server].Call("Raft.RequestVote", request, response)
	if !ok {
		utils.Debug(utils.DWarn, "S%d call (RequestVote)rpc to C%d error", rf.me, server)
		return ok
	}
	utils.Debug(utils.DInfo, "S%d get RequestVote response from %d {%+v}", rf.me, server, response)

	return ok
}

func (rf *Raft) sendAppendEntries(server int, request *AppendEntriesArgs, response *AppendEntriesReply) bool {
	utils.Debug(utils.DInfo, "S%d send AppendEntries request to %d {%+v}", rf.me, server, request)
	ok := rf.peers[server].Call("Raft.AppendEntries", request, response)
	if !ok {
		utils.Debug(utils.DWarn, "S%d call (AppendEntries)rpc to C%d error", rf.me, server)
		return ok
	}
	utils.Debug(utils.DInfo, "S%d get AppendEntries response from %d {%+v}", rf.me, server, response)
	return ok
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

	// Your code here (2B).
	if rf.state != leaderState {
		utils.Debug(utils.DClient, "S%d Not leader cmd: %+v", rf.me, command)
		return -1, -1, false
	}

	index := rf.lastLogIndex() + 1
	rf.log = append(rf.log, Entry{index, rf.currentTerm, command})
	rf.persist()

	utils.Debug(utils.DClient, "S%d cmd: %+v, logIndex: %d", rf.me, command, rf.lastLogIndex())
	rf.doAppendEntries()

	return rf.lastLogIndex(), rf.currentTerm, true
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
		peers:     peers,
		persister: persister,
		me:        me,
		applyCh:   applyCh,
	}

	// Your initialization code here (2A, 2B, 2C).
	rf.init()

	utils.Debug(utils.DClient, "S%d Started && init success", rf.me)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	// start goroutine to write its own entry
	go rf.applyLog()
	return rf
}

func (rf *Raft) init() {
	rf.state = FollowerState
	rf.currentTerm = 0
	rf.votedFor = Voted_nil
	rf.log = make([]Entry, 0)

	// use first log entry as last snapshot index
	rf.log = append(rf.log, Entry{magic_index, magic_term, nil})

	// volatile for all servers
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.resetElectionTime()

}
