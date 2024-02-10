package kvraft

import (
	"6.824/labrpc"
	"6.824/utils"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderId int
	seqId    int64
	clientId int64
}

const CommandRPC = "KVServer.Command"

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.leaderId = 0
	ck.clientId = nrand()
	ck.seqId = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//

func (ck *Clerk) sendCmd(key string, value string, OPType OPType) string {
	ck.seqId += 1
	args := CmdArgs{
		OPType:   OPType,
		Key:      key,
		Value:    value,
		SeqId:    ck.seqId,
		ClientId: ck.clientId,
	}

	t0 := time.Now()
	for time.Since(t0).Seconds() < 10 {
		reply := CmdReply{}

		ok := ck.servers[ck.leaderId].Call(CommandRPC, &args, &reply)

		if !ok {
			utils.Debug(utils.DWarn, "Fail to talk to %v, trying to talk to the next server %v", ck.leaderId, (ck.leaderId+1)%len(ck.servers))
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			time.Sleep(RetryTimeout)
			continue
		}

		if reply.Err == OK {
			return reply.Value
		} else if reply.Err == ErrNoKey {
			return ""
		}
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		time.Sleep(RetryTimeout)
	}
	panic("There is no response in the last 10 seconds, something wrong happened, terminating the process")
	return ""
}
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	return ck.sendCmd(key, "", OpGet)
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//

func (ck *Clerk) Put(key string, value string) {
	ck.sendCmd(key, value, OpPut)
}
func (ck *Clerk) Append(key string, value string) {
	ck.sendCmd(key, value, OpAppend)
}
