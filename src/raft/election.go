package raft

import "6.824/utils"

func (rf *Raft) doElection() {
	votedCount := 1
	entry := rf.lastLog()
	request := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogTerm:  entry.Term,
		LastLogIndex: entry.Index,
	}

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go func(i int) {
			response := RequestVoteReply{}
			ok := rf.sendRequestVote(i, &request, &response)
			if !ok {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			// If RPC request or response contains term T > currentTerm, set currentTerm = T, convert to follower
			if response.Term > rf.currentTerm {
				utils.Debug(utils.DTerm, "S%d S%d term larger(%d > %d)", rf.me, i, response.Term, rf.currentTerm)

				rf.currentTerm, rf.votedFor = response.Term, Voted_nil
				rf.persist()
				rf.ChangeState(FollowerState)
				return
			}

			if response.VoteGranted {
				votedCount++
				// node becomes leader if votes received from the majority of servers
				if votedCount > len(rf.peers)/2 && rf.state == candidateState {
					rf.ChangeState(leaderState)
				}
			}
		}(i)
	}
}

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

func (rf *Raft) leaderInit() {
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.lastApplied + 1
		rf.matchIndex[i] = 0
	}

	rf.resetHeartbeatTime()

}
