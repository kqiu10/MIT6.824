package kvraft

import "time"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

type OPType string

const (
	OpGet    OPType = "Get"
	OpPut    OPType = "Put"
	OpAppend OPType = "Append"
)

type CmdArgs struct {
	OPType   OPType
	Key      string
	Value    string
	ClientId int64
	SeqId    int64
}

type CmdReply struct {
	Err   Err
	Value string
}

const (
	RetryTimeout    time.Duration = time.Duration(1) * time.Millisecond
	CmdTimeout      time.Duration = time.Duration(2) * time.Second
	GapTime         time.Duration = time.Duration(5) * time.Millisecond
	SnapshotGapTime time.Duration = time.Duration(10) * time.Millisecond
)
