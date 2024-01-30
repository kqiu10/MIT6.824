package raft

import "6.824/utils"

// ticker() call doAppendEntries(), ticker() hold lock
// if a node turn to a leader, leader will call doAppendEntries() to send heartbeat
func (rf *Raft) doAppendEntries() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		wantSendIndex := rf.nextIndex[i] - 1
		if wantSendIndex < rf.frontLogIndex() {
			go rf.doInstallSnapshot(i)
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

	if err < 0 {
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

	// ignore this transition if state changed or outdated data
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

	// handle exceptions
	if response.XTerm == -1 {
		// null slot
		rf.nextIndex[peer] -= response.XLen
	} else if response.XTerm >= 0 {
		termNotExit := true
		for index := rf.nextIndex[peer] - 1; index >= 1; index-- {
			entry, err := rf.getEntry(index)
			if err < 0 {
				continue
			}

			if entry.Term > response.XTerm {
				continue
			}
			// Found the first mismatch
			if entry.Term == response.XTerm {
				rf.nextIndex[peer] = index + 1
				termNotExit = false
				break
			}

			// Only one mismatch
			if entry.Term < response.XTerm {
				break
			}
		}
		if termNotExit {
			rf.nextIndex[peer] = response.XIndex
		}
	} else {
		rf.nextIndex[peer] = response.XIndex
	}

	// corner case: the smallest nextIndex >= 1
	if rf.nextIndex[peer] < 1 {
		rf.nextIndex[peer] = 1
	}

}

func (rf *Raft) toCommit() {
	// check if entries are appended in the majority of nodes after entries appended in a random node
	// if true, commit the transition.
	if rf.commitIndex >= rf.lastLogIndex() {
		return
	}

	for i := rf.lastLogIndex(); i > rf.commitIndex; i-- {
		entry, err := rf.getEntry(i)
		if err < 0 {
			continue
		}

		if entry.Term != rf.currentTerm {
			return
		}

		cnt := 1 // count self
		for j, match := range rf.matchIndex {
			if j != rf.me && match >= i {
				cnt++
			}

			if cnt > len(rf.peers)/2 {
				rf.commitIndex = i
				utils.Debug(utils.DCommit, "S%d commit to %v", rf.me, rf.commitIndex)
				return
			}
		}

	}
	utils.Debug(utils.DCommit, "S%d don't have half replicated from %v to %v now", rf.me, rf.commitIndex, rf.lastLogIndex())
}
