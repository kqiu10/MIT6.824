package raft

import "6.824/utils"

func (rf *Raft) InstallSnapshot(request *InstallSnapshotArgs, response *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	utils.Debug(utils.DSnap, "S%d S%d installSnapshot", rf.me, request.LeaderId)
	defer utils.Debug(utils.DSnap, "S%d arg: %+v reply: %+v", rf.me, request, response)

	if request.Term < rf.currentTerm {
		response.Term = rf.currentTerm
		return
	}

	if request.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = request.Term, request.LeaderId
		rf.persist()
		rf.ChangeState(FollowerState)
	}

	if rf.state != FollowerState {
		rf.ChangeState(FollowerState)
	}

	response.Term = rf.currentTerm
	rf.resetElectionTime()

	if request.LastIncludedIndex <= rf.commitIndex {
		utils.Debug(utils.DSnap, "S%d args's snapshot too old(%d < %d)", rf.me, request.LastIncludedIndex, rf.commitIndex)
		return
	}

	go func() {
		rf.applyCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      request.Data,
			SnapshotTerm:  request.LastIncludedTerm,
			SnapshotIndex: request.LastIncludedIndex,
		}
	}()
}
