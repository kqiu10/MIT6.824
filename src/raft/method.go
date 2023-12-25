package raft

import (
	"6.824/utils"
	"math/rand"
	"time"
)

func (rf *Raft) ChangeState(state State) {
	switch state {
	case FollowerState:
		rf.state = FollowerState
		utils.Debug(utils.DInfo, "S%d converting to %v in T(%d)", rf.me, rf.state, rf.currentTerm)

	case candidateState:
		rf.currentTerm++
		// select itself as leader
		rf.votedFor = rf.me
		rf.persist()
		rf.state = candidateState
		utils.Debug(utils.DTerm, "S%d converting to %v in T(%d)", rf.me, rf.state, rf.currentTerm)

	case leaderState:
		rf.state = leaderState
		rf.leaderInit()
		utils.Debug(utils.DTerm, "S%d converting to %v in T(%d)", rf.me, rf.state, rf.currentTerm)
		// Send initial empty AppendEntries RPCs to each server, and repeat during idle periods to prevent election timeouts
		rf.doAppendEntries()
	}
}

func (rf *Raft) lastLog() Entry {
	return rf.log[len(rf.log)-1]
}

func (rf *Raft) isUpToDate(lastLogIndex int, lastLogTerm int) bool {
	entry := rf.lastLog()
	index := entry.Index
	term := entry.Term

	if term == lastLogTerm {
		return lastLogIndex >= index
	}
	return lastLogTerm > term

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

func (rf *Raft) leaderInit() {
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.lastApplied + 1
		rf.matchIndex[i] = 0
	}

	rf.resetHeartbeatTime()

}

func (rf *Raft) electionTimeout() bool {
	return time.Now().After(rf.electionTime)
}

func (rf *Raft) heartbeatTimeout() bool {
	return time.Now().After(rf.heartbeatTime)
}

// Generate randomly election timeouts to avoid all candidates votes for one leader at the same time.
func (rf *Raft) resetElectionTime() {
	election_interval := rand.Intn(election_range_time) + election_base_time
	rf.electionTime = time.Now().Add(time.Duration(election_interval) * time.Millisecond)
}

func (rf *Raft) resetHeartbeatTime() {
	rf.heartbeatTime = time.Now().Add(time.Duration(heartbeat_time) * time.Millisecond)
}
