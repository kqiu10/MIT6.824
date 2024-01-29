package raft

import "6.824/utils"

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(request *RequestVoteArgs, response *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	utils.Debug(utils.DVote, "S%d C%d asking vote", rf.me, request.CandidateId)

	defer rf.persist()

	if request.Term < rf.currentTerm {
		utils.Debug(utils.DVote, "S%d Term is higher than C%d, decline it", rf.me, request.CandidateId)
		response.Term, response.VoteGranted = rf.currentTerm, false
		return
	}
	if request.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = request.Term, Voted_nil
		utils.Debug(utils.DVote, "S%d Term is lower than C%d, turn to follower && reset voted_for", rf.me, request.CandidateId)
		rf.ChangeState(FollowerState)
	}
	if rf.votedFor == Voted_nil || rf.votedFor == request.CandidateId {
		if !rf.isUpToDate(request.LastLogIndex, request.LastLogTerm) {
			utils.Debug(utils.DVote, "S%d C%d not up-to-date, decline it{arg:%+v, index:%d term:%d}",
				rf.me, request.CandidateId, request, rf.lastLog().Index, rf.lastLog().Term)
			response.Term, response.VoteGranted = rf.currentTerm, false
			return
		}

		rf.votedFor = request.CandidateId
		response.VoteGranted = true
		response.Term = rf.currentTerm
		utils.Debug(utils.DVote, "S%d Granting Vote to S%d at T%d", rf.me, rf.votedFor, rf.currentTerm)
		rf.resetElectionTime()
		return

	}

	// have voted
	response.Term, response.VoteGranted = rf.currentTerm, false
	utils.Debug(utils.DVote, "S%d Have voted to S%d at T%d, refuse S%d", rf.me, rf.votedFor, rf.currentTerm, request.CandidateId)
	return
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
