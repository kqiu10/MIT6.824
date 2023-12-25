package raft

func (rf *Raft) ChangeState(state State) {
	rf.state = state
}

func (rf *Raft) lastLog() Entry {
	return rf.log[len(rf.log)-1]
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
