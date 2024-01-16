package raft

import (
	"6.824/utils"
)

// followers handle entries sent by leader and try to achieve consensus, handler need to require lock
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	utils.Debug(utils.DLog, "S%d S%d appendEntries", rf.me, args.LeaderId)
	defer utils.Debug(utils.DLog, "S%d arg: %+v reply: %+v", rf.me, args, reply)

	defer rf.persist()

	if args.Term < rf.currentTerm {
		// node is not leader, refuse the request
		reply.Term = rf.currentTerm
		reply.Success = false
		utils.Debug(utils.DTerm, "S%d S%d term less(%d < %d)", rf.me, args.LeaderId, args.Term, rf.currentTerm)
		return
	}

	if args.Term > rf.currentTerm {
		// If RPC request or response contains term T > currentTerm:
		// set currentTerm = T, convert to follower (§5.1)
		rf.currentTerm, rf.votedFor = args.Term, Voted_nil
		utils.Debug(utils.DTerm, "S%d S%d term larger(%d > %d)", rf.me, args.LeaderId, args.Term, rf.currentTerm)
		rf.ChangeState(FollowerState)
	}

	if rf.state != FollowerState {
		rf.ChangeState(FollowerState)
	}

	reply.Success = true
	reply.Term = rf.currentTerm
	// prevent election timeouts
	rf.resetElectionTime()

	if args.PrevLogIndex < rf.frontLogIndex() {
		reply.XTerm, reply.XIndex, reply.Success = -2, rf.frontLogIndex()+1, false
		utils.Debug(utils.DInfo, "S%d args's prevLogIndex too smaller(%v < %v)", rf.me, args.PrevLogIndex, rf.frontLogIndex())
		return
	}

	if args.PrevLogIndex > rf.lastLogIndex() {
		reply.Success = false
		reply.XTerm = -1
		reply.XLen = args.PrevLogIndex - rf.lastLogIndex()
		return
	}

	index, err := rf.transfer(args.PrevLogIndex)
	if err < 0 {
		return
	}

	if rf.log[index].Term != args.PrevLogTerm {
		reply.Success = false
		reply.XTerm = rf.log[index].Term
		reply.XIndex = args.PrevLogIndex
		// In case same term duplicates in log entry, iterate from index to head to find the first difference.
		for i := index; i > rf.frontLogIndex(); i-- {
			if rf.log[i-1].Term != reply.XTerm {
				reply.XIndex = i
				break
			}
		}
		return
	}

	// handle batch entries update
	if args.Entries != nil && len(args.Entries) != 0 {
		if rf.isConflict(args) {
			rf.log = rf.log[:index+1]
			entries := make([]Entry, len(args.Entries))
			copy(entries, args.Entries)
			rf.log = append(rf.log, entries...)
		} else {
			utils.Debug(utils.DInfo, "S%d no conflict, log: %+v", rf.me, rf.log)
		}
	} else {
		utils.Debug(utils.DInfo, "S%d args entries nil or length is 0: %v", rf.me, args.Entries)
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit
		if args.LeaderCommit > rf.lastLogIndex() {
			rf.commitIndex = rf.lastLogIndex()
		}
		utils.Debug(utils.DCommit, "S%d commit to %v(lastLogIndex: %d)", rf.me, rf.commitIndex, rf.lastLogIndex())
	}
}

func (rf *Raft) isConflict(args *AppendEntriesArgs) bool {

	for idx, entry := range args.Entries {
		rf_entry, err := rf.getEntry(idx)
		if err < 0 {
			return true
		}
		if rf_entry.Term != entry.Term {
			return true
		}
	}

	return false
}
