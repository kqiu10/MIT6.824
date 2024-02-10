package kvraft

import (
	"6.824/utils"
	"time"
)

func (kv *KVServer) Command(args *CmdArgs, reply *CmdReply) {
	defer utils.Debug(utils.DWarn, "S%d args: %+v reply: %+v", kv.me, args, reply)

	kv.mu.Lock()
	if args.OPType != OpGet && kv.isDuplicate(args.ClientId, args.SeqId) {
		context := kv.LastCmdContext[args.ClientId]
		reply.Value, reply.Err = context.Reply.Value, context.Reply.Err
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	cmd := Op{
		OpType:   args.OPType,
		Key:      args.Key,
		Value:    args.Value,
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
	}
	index, term, is_leader := kv.rf.Start(cmd)

	if !is_leader {
		reply.Value, reply.Err = "", ErrWrongLeader
		return
	}

	kv.mu.Lock()
	it := IndexAndTerm{index, term}
	ch := make(chan OpResp, 1)
	kv.cmdRespChans[it] = ch
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		delete(kv.cmdRespChans, it)
		kv.mu.Unlock()
		close(ch)
	}()

	t := time.NewTimer(CmdTimeout)
	defer t.Stop()

	for {
		kv.mu.Lock()
		select {
		case resp := <-ch:
			utils.Debug(utils.DServer, "S%d have applied, resp: %+v", kv.me, resp)
			reply.Value, reply.Err = resp.Value, resp.Err
			kv.mu.Unlock()
			return
		case <-t.C:
		lastTry:
			for {
				select {
				case resp := <-ch:
					utils.Debug(utils.DServer, "S%d have applied, resp: %+v", kv.me, resp)
					reply.Value, reply.Err = resp.Value, resp.Err
					kv.mu.Unlock()
					return
				default:
					break lastTry
				}
			}
			utils.Debug(utils.DServer, "S%d timeout", kv.me)
			reply.Value, reply.Err = "", ErrTimeout
			kv.mu.Unlock()
			return
		default:
			kv.mu.Unlock()
			time.Sleep(GapTime)
		}
	}
}
