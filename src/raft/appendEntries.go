package raft

import "6.824/utils"

// if a node turn to a leader, leader will call doAppendEntries() to send heartbeat
func (rf *Raft) doAppendEntries() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		sendIndex := rf.nextIndex[i] - 1
		if sendIndex < rf.frontLogIndex() {
			utils.Debug(utils.DError, "S%d S%d index smaller than 0 (%d < 0)", rf.me, i, sendIndex)
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

}
