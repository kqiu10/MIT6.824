package raft

// type of Servers
const (
	leaderState    State = "Leader"
	FollowerState  State = "Follower"
	candidateState State = "Candidate"
)

// election
const (
	Voted_nil int = -12345
)

// appendEntries
const (
	magic_index int = 0
	magic_term  int = -1
)

// log Entry
type Entry struct {
	Index int
	Term  int
	Cmd   interface{}
}

// ticker
const (
	gap_time            int = 30
	election_base_time  int = 300
	election_range_time int = 100
	heartbeat_time      int = 50
)
