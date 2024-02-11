package kvraft

import (
	"6.824/labgob"
	"6.824/utils"
	"bytes"
	"log"
)

type KV struct {
	Kvmap map[string]string
}

func (kv *KV) Get(key string) (string, Err) {
	// Your code here.
	if value, ok := kv.Kvmap[key]; ok {
		return value, OK
	}
	return "", ErrNoKey
}

func (kv *KV) Put(key string, value string) Err {
	kv.Kvmap[key] = value
	return OK
}

func (kv *KV) Append(key string, value string) Err {
	if value_ori, ok := kv.Kvmap[key]; ok {
		kv.Kvmap[key] = value_ori + value
		return OK
	}
	kv.Kvmap[key] = value
	return OK
}

func NewKV() *KV {
	return &KV{make(map[string]string)}
}

func (kv *KVServer) setSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	utils.Debug(utils.DServer, "S%d setSnapshot", kv.me)
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var kvMap KV
	var lastCmdContext map[int64]OpContext

	if d.Decode(&kvMap) != nil || d.Decode(&lastCmdContext) != nil {
		log.Fatal("server setSnapshot decode error \n")
	} else {
		kv.KvMap = &kvMap
		kv.LastCmdContext = lastCmdContext
	}
}

func (kv *KVServer) Opt(cmd Op) (string, Err) {
	switch cmd.OpType {
	case OpGet:
		value, err := kv.KvMap.Get(cmd.Key)
		return value, err
	case OpPut:
		err := kv.KvMap.Put(cmd.Key, cmd.Value)
		return "", err
	case OpAppend:
		err := kv.KvMap.Append(cmd.Key, cmd.Value)
		return "", err
	default:
		return "", OK
	}
}
