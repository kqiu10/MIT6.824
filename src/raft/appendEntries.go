package raft

import "6.824/utils"

// if a node turn to a leader, leader will call doAppendEntries() to send heartbeat
func (rf *Raft) doAppendEntries() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		wantSendIndex := rf.nextIndex[i] - 1
		if wantSendIndex < rf.frontLogIndex() {
			utils.Debug(utils.DError, "S%d S%d index smaller than 0 (%d < 0)", rf.me, i, wantSendIndex)
			continue
		} else {
			go rf.appendTo(i)
		}
	}
}

func (rf *Raft) appendTo(peer int) {
	rf.mu.Lock()
	if rf.state != leaderState {
		utils.Debug(utils.DWarn, "S%d status change, it is not leader", rf.me)
		rf.mu.Unlock()
		return
	}
	request := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: magic_index,
		PrevLogTerm:  magic_term,
		LeaderCommit: rf.commitIndex,
	}

	prevLogIndex := rf.nextIndex[peer] - 1
	idx, err := rf.transfer(prevLogIndex)

	if err == -1 {
		rf.mu.Unlock()
		return
	}

	request.PrevLogIndex = rf.log[idx].Index
	request.PrevLogTerm = rf.log[idx].Term

	// copy entries in leader from idx to the end
	entries := rf.log[idx+1:]
	request.Entries = make([]Entry, len(entries))
	copy(request.Entries, entries)
	rf.mu.Unlock()

	response := AppendEntriesReply{}

	ok := rf.sendAppendEntries(peer, &request, &response)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// state changed or outdue data, ignore
	if rf.currentTerm != request.Term || rf.state != leaderState || response.Term < rf.currentTerm {
		// overdue, ignore
		utils.Debug(utils.DInfo, "S%d old response from C%d, ignore it", rf.me, peer)
		return
	}

	// If AppendEntries RPC response contains term T > currentTerm:
	// set currentTerm = T, convert to follower
	if response.Term > rf.currentTerm {
		utils.Debug(utils.DTerm, "S%d S%d term larger(%d > %d)", rf.me, peer, response.Term, rf.currentTerm)
		rf.currentTerm, rf.votedFor = response.Term, Voted_nil
		rf.persist()
		rf.ChangeState(FollowerState)
		return
	}

	if response.Success {
		rf.nextIndex[peer] = request.PrevLogIndex + len(request.Entries) + 1
		rf.matchIndex[peer] = request.PrevLogIndex + len(request.Entries)
		rf.toCommit()
		return
	}

}
