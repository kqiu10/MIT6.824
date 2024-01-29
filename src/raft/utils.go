package raft

import "6.824/utils"

// type of Servers
const (
	leaderState    State = "Leader"
	FollowerState  State = "Follower"
	candidateState State = "Candidate"
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

// election
const (
	Voted_nil int = -12345
)

// appendEntries
const (
	magic_index int = 0
	magic_term  int = -1
)

// log Entry
type Entry struct {
	Index int
	Term  int
	Cmd   interface{}
}

// ticker
const (
	gap_time            int = 3
	election_base_time  int = 300
	election_range_time int = 100
	heartbeat_time      int = 50
)
