package raft

import (
	"6.824/utils"
	"math/rand"
	"time"
)

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

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.Lock()
		switch rf.state {
		case FollowerState:
			if rf.electionTimeout() {
				rf.ChangeState(candidateState)
				utils.Debug(utils.DInfo, "S%d Election timeout, starting election, T%d", rf.me, rf.currentTerm)
				rf.doElection()
				rf.resetElectionTime()
			}
		case candidateState:
			if rf.electionTimeout() {
				rf.ChangeState(candidateState)
				utils.Debug(utils.DInfo, "S%d Election timeout, starting election, T%d", rf.me, rf.currentTerm)
				rf.doElection()
				rf.resetElectionTime()
			}
		case leaderState:
			if rf.heartbeatTimeout() {
				utils.Debug(utils.DInfo, "S%d heartbeat timeout, starting sending heartbeat, T%d", rf.me, rf.currentTerm)
				rf.doAppendEntries()
				rf.resetHeartbeatTime()
			}

		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(gap_time) * time.Millisecond)

	}
}
