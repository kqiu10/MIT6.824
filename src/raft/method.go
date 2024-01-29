package raft

import (
	"6.824/utils"
)

func (rf *Raft) lastLog() Entry {
	return rf.log[len(rf.log)-1]
}

func (rf *Raft) frontLogIndex() int {
	return rf.log[0].Index
}

func (rf *Raft) lastLogIndex() int {
	return rf.log[len(rf.log)-1].Index
}

func (rf *Raft) transfer(index int) (int, int) {
	begin := rf.frontLogIndex()
	end := rf.lastLogIndex()

	if index < begin || index > end {
		utils.Debug(utils.DWarn, "S%d log out of range: %d, [%d, %d]", rf.me, index, begin, end)
		return 0, -1
	}
	return index - begin, 0
}

func (rf *Raft) getEntry(index int) (Entry, int) {
	begin := rf.frontLogIndex()
	end := rf.lastLogIndex()

	if index < begin || index > end {
		utils.Debug(utils.DWarn, "S%d log out of range: %d, [%d, %d]", rf.me, index, begin, end)
		return Entry{magic_index, magic_term, nil}, -1
	}
	return rf.log[index-begin], 0
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

func (rf *Raft) toCommit() {
	// append entries before commit
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
