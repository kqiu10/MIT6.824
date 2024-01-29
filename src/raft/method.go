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
