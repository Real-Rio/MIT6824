package kvraft

import (
	// "log"
	"sync"
	"sync/atomic"
	"time"

	"MIT6824/labgob"
	"MIT6824/labrpc"
	"MIT6824/raft"
)

// const Debug = false

// func DPrintf(format string, a ...interface{}) (n int, err error) {
// 	if Debug {
// 		log.Printf(format, a...)
// 	}
// 	return
// }

type Op struct {
	Operation string // "Put" "Append" "Get"
	ClientID  int64
	MsgID     int
	Key       string
	Value     string
}

type executeResult struct {
	err   Err
	value string
}

type KVServer struct {
	mu      sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	notifyChan map[int]chan executeResult

	kvStore map[string]string // key-value store
	lastEx  map[int64]int     // clientID -> last executed messageID
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	command := Op{Operation: "Get", ClientID: args.ClientID, MsgID: args.MsgID, Key: args.Key}
	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		Debug(dWarn, "S%d get not leader",kv.me)
		reply.Err = ErrWrongLeader
		reply.Value = ""
		return
	}
	Debug(dServer, "S%d Receive Get %s,index %d",kv.me, args.Key, index)

	kv.mu.Lock()
	kv.notifyChan[index] = make(chan executeResult, 1)
	kv.mu.Unlock()

	select {
	case res := <-kv.notifyChan[index]:
		if res.err == ErrNoKey {
			Debug(dWarn, "No key")
			reply.Err = ErrNoKey
		} else {
			Debug(dWarn, "S%d Get success",kv.me)
			reply.Err = OK
			reply.Value = res.value
		}
		kv.mu.Lock()
		close(kv.notifyChan[index])
		delete(kv.notifyChan, index)
		kv.mu.Unlock()
	case <-time.After(1000 * time.Millisecond):
		Debug(dWarn, "S%d get Timeout",kv.me)
		reply.Err = TimeOut
	}

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	command := Op{Operation: args.Op, ClientID: args.ClientID, MsgID: args.MsgID, Key: args.Key, Value: args.Value}
	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		Debug(dWarn, "S%d PutAppend not leader",kv.me)
		reply.Err = ErrWrongLeader
		return
	}

	Debug(dServer, "S%d Receive PutAppend %s %s,index %d",kv.me, args.Key, args.Value, index)

	kv.mu.Lock()
	kv.notifyChan[index] = make(chan executeResult, 1)
	kv.mu.Unlock()

	select {
	case <-kv.notifyChan[index]:
		Debug(dWarn, "S%d PutAppend success",kv.me)
		reply.Err = OK
	case <-time.After(1000 * time.Millisecond):
		Debug(dWarn, "S%d put append Timeout",kv.me)
		reply.Err = TimeOut
	}
}

func (kv *KVServer) applyLoop() {
	for apply := range kv.applyCh {
		if kv.killed() {
			return
		}
		if !apply.CommandValid {
			continue
		}

		op := apply.Command.(Op)
		clientID := op.ClientID
		msgID := op.MsgID
		executeResult := executeResult{}

		kv.mu.Lock()
		if op.Operation == "Put" && msgID > kv.lastEx[clientID] {
			Debug(dLog, "S%d new put index:%d",kv.me, apply.CommandIndex)
			kv.kvStore[op.Key] = op.Value
			kv.lastEx[clientID] = msgID
		} else if op.Operation == "Append" && msgID > kv.lastEx[clientID] {
			Debug(dLog, "S%d new append index:%d",kv.me, apply.CommandIndex)
			kv.kvStore[op.Key] += op.Value
			kv.lastEx[clientID] = msgID
		} else if op.Operation == "Get" {
			Debug(dLog, "S%d new get index:%d",kv.me, apply.CommandIndex)
			if _, ok := kv.kvStore[op.Key]; !ok {
				executeResult.err = ErrNoKey
			} else {
				executeResult.value = kv.kvStore[op.Key]
			}
			if msgID > kv.lastEx[clientID] {
				kv.lastEx[clientID] = msgID
			}
		}

		// notify rpc handler
		if _, ok := kv.notifyChan[apply.CommandIndex]; ok {
			kv.notifyChan[apply.CommandIndex] <- executeResult
		}
		kv.mu.Unlock()
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.kvStore = make(map[string]string)
	kv.notifyChan = make(map[int]chan executeResult)
	kv.lastEx = make(map[int64]int)
	go kv.applyLoop()
	return kv
}
