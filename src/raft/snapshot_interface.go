package raft

import "6.824/utils"

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// had more recent info since it communicate the snapshot on applyCh.
// lastIncludedIndex the snapshot replaces all entries up through and including this index

// InstallSnapshot RPCs are sent between Raft peers,
// whereas the provided skeleton functions Snapshot/CondInstallSnapshot are used by the service to communicate to Raft.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	rf.mu.Lock()

	defer rf.mu.Unlock()
	utils.Debug(utils.DSnap, "S%d CondInstallSnapshot(lastIncludedTerm: %d lastIncludedIndex: %d lastApplied: %d commitIndex: %d)", rf.me, lastIncludedTerm, lastIncludedIndex, rf.lastApplied, rf.commitIndex)

	if lastIncludedIndex <= rf.commitIndex {
		utils.Debug(utils.DSnap, "S%d refuse, snapshot too old(%d < %d)", rf.me, lastIncludedIndex, rf.commitIndex)
		return false
	}

	if lastIncludedIndex > rf.lastLogIndex() {
		rf.log = make([]Entry, 1)
	} else {
		idx, _ := rf.transfer(lastIncludedIndex)
		//if err < 0 {
		//	utils.Debug(utils.DSnap, "S%d refuse, index %d out of range", rf.me, lastIncludedIndex)
		//	return false
		//}
		rf.log = rf.log[idx:]
	}

	// dummy node
	rf.log[0].Index = lastIncludedIndex
	rf.log[0].Term = lastIncludedTerm
	rf.log[0].Cmd = nil

	rf.persistSnapshot(snapshot)

	// reset commit & lastApplied
	rf.lastApplied = lastIncludedIndex
	rf.commitIndex = lastIncludedIndex

	utils.Debug(utils.DSnap, "S%d after CondInstallSnapshot(lastApplied: %d commitIndex: %d) {%+v}", rf.me, rf.lastApplied, rf.commitIndex, rf.log)

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	utils.Debug(utils.DSnap, "S%d call Snapshot, index: %d", rf.me, index)

	// refuse to install a snapshot
	if rf.frontLogIndex() >= index {
		utils.Debug(utils.DSnap, "S%d refuse, have received %d snapshot", rf.me, index)
		return
	}

	idx, err := rf.transfer(index)
	if err < 0 {
		idx = len(rf.log) - 1
	}

	// make last snapshot node as dummy node
	rf.log = rf.log[idx:]
	rf.log[0].Cmd = nil
	rf.persistSnapshot(snapshot)
}
