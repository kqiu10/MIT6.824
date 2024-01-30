package raft

import "6.824/utils"

func (rf *Raft) doInstallSnapshot(peer int) {
	rf.mu.Lock()

	if rf.state != leaderState {
		utils.Debug(utils.DWarn, "S%d state change, it is not leader", rf.me)
		rf.mu.Unlock()
		return
	}

	request := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.frontLogIndex(),
		LastIncludedTerm:  rf.frontLogTerm(),
	}

	request.Data = make([]byte, rf.persister.SnapshotSize())
	copy(request.Data, rf.persister.ReadSnapshot())
	rf.mu.Unlock()

	response := InstallSnapshotReply{}

	ok := rf.sendInstallSnapshot(peer, &request, &response)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// state changed or outdue data, ignore
	if rf.currentTerm != request.Term || rf.state != leaderState || response.Term < rf.currentTerm {
		utils.Debug(utils.DInfo, "S%d old response from C%d, ignore it", rf.me, peer)
		return
	}

	if response.Term > rf.currentTerm {
		utils.Debug(utils.DTerm, "S%d S%d term larger(%d > %d)", rf.me, peer, response.Term, rf.currentTerm)
		rf.ChangeState(FollowerState)
		rf.currentTerm, rf.votedFor = response.Term, Voted_nil
		rf.persist()
		return
	}

	rf.nextIndex[peer] = request.LastIncludedIndex + 1

	utils.Debug(utils.DInfo, "S%d send snapshot to C%d success!", rf.me, peer)
}
