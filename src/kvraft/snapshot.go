package kvraft

import (
	"6.824/labgob"
	"6.824/utils"
	"bytes"
	"log"
	"time"
)

func (kv *KVServer) passSnapshot() {
	for kv.killed() == false {
		kv.mu.Lock()
		if kv.isNeedSnapshot() && kv.lastApplied > kv.lastSnapshot+snapshotLogGap {
			kv.doSnapshot(kv.lastApplied)
			kv.lastSnapshot = kv.lastApplied
		}
		kv.mu.Unlock()
		time.Sleep(SnapshotGapTime)
	}
}

func (kv *KVServer) isNeedSnapshot() bool {
	if kv.maxraftstate != -1 && kv.rf.RaftPersistSize() > int(snapshotThreshold*float32(kv.maxraftstate)) {
		return true
	}
	return false
}

func (kv *KVServer) doSnapshot(commandIndex int) {
	utils.Debug(utils.DServer, "S%d doSnapshot", kv.me)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(*kv.KvMap) != nil || e.Encode(kv.LastCmdContext) != nil {
		panic("server doSnapshot encode error")
	}
	kv.rf.Snapshot(commandIndex, w.Bytes())
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
