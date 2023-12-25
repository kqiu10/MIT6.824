package raft

import (
	"fmt"
	"log"
	"time"
)

// type of Servers
const (
	leaderState    State = "Leader"
	followerState  State = "Follower"
	candidateState State = "Candidate"
)

// election
const (
	voted_nil int = -12345
)

// log Entry
type Entry struct {
	Index int
	Term  int
	Cmd   interface{}
}

// Debugging
const debug = false

type logTopic string

const (
	DClient  logTopic = "CLNT"
	DCommit  logTopic = "CMIT"
	DDrop    logTopic = "DROP"
	DError   logTopic = "ERRO"
	DInfo    logTopic = "INFO"
	DLeader  logTopic = "LEAD"
	DLog     logTopic = "LOG1"
	DLog2    logTopic = "LOG2"
	DPersist logTopic = "PERS"
	DSnap    logTopic = "SNAP"
	DTerm    logTopic = "TERM"
	dTest    logTopic = "TEST"
	DTimer   logTopic = "TIMR"
	DTrace   logTopic = "TRCE"
	DVote    logTopic = "VOTE"
	DWarn    logTopic = "WARN"
	DServer  logTopic = "SEVR"
)

var debugStart time.Time

func init() {
	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func Debug(topic logTopic, format string, a ...interface{}) {
	if debug {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}
