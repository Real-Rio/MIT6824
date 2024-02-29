package shardkv

import (
	"sync"
	"time"

	"MIT6824/labgob"
	"MIT6824/labrpc"
	"MIT6824/raft"
	"MIT6824/shardctrler"
)

type ShardKV struct {
	mu           sync.RWMutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	lastConfig    shardctrler.Config
	currentConfig shardctrler.Config
	stateMachines map[int]*Shard
	NotifyChan    map[int]chan CommandResponse

	sc *shardctrler.Clerk

	retry     int
	LastMsgID map[int64]int // clientID -> last executed messageID

}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	shardID := key2shard(args.Key)
	shardStatus := kv.canServe(shardID)
	if shardStatus != OK {
		reply.Err = shardStatus
		reply.Value = ""
		return
	}

	op := Op{Operation: "Get", ClientID: args.ClientID, MsgID: args.MsgID, Key: args.Key}
	response := CommandResponse{}

	kv.Execute(NewOperationCommand(&op), &response)

	reply.Err = response.Err
	reply.Value = response.Value

}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	shardID := key2shard(args.Key)
	shardStatus := kv.canServe(shardID)
	if shardStatus != OK {
		reply.Err = shardStatus
		return
	}

	op := Op{Operation: args.Op, ClientID: args.ClientID, MsgID: args.MsgID, Key: args.Key, Value: args.Value}
	command := Command{Op: Operation, Data: op}
	response := CommandResponse{}

	kv.Execute(command, &response)

	reply.Err = response.Err
}

func (kv *ShardKV) Execute(command Command, reply *CommandResponse) {
	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	if kv.NotifyChan[index] == nil {
		kv.NotifyChan[index] = make(chan CommandResponse, 1)
	}
	notifyChan := kv.NotifyChan[index]
	kv.mu.Unlock()

	select {
	case res := <-notifyChan:
		reply.Err = res.Err
		reply.Value = res.Value
	case <-time.After(500 * time.Millisecond):
		reply.Err = TimeOut
	}

	go func() {
		kv.mu.Lock()
		close(kv.NotifyChan[index])
		delete(kv.NotifyChan, index)
		kv.mu.Unlock()
	}()
}

func (kv *ShardKV) applyLoop() {
	for apply := range kv.applyCh {
		kv.mu.Lock()
		if kv.Killed() {
			kv.mu.Unlock()
			return
		}

		// if apply.SnapshotValid {
		// 	Debug(dSnap, "S%d restore snapshot", kv.me)
		// 	kv.readSnapshot(apply.Snapshot)
		// 	continue
		// }

		if !apply.CommandValid {
			continue
		}

		command := apply.Command.(Command)
		// executeResult := executeResult{}
		var response *CommandResponse

		switch command.Op {
		case Operation:
			operation := command.Data.(Op)
			response = kv.applyOperation(&operation)
		case Configuration:
			config := command.Data.(shardctrler.Config)
			response = kv.applyConfiguration(&config)
		}

		term, isLeader := kv.rf.GetState()

		// notify rpc handler
		if _, ok := kv.NotifyChan[apply.CommandIndex]; ok && isLeader && term == apply.CommandTerm {
			kv.NotifyChan[apply.CommandIndex] <- *response
		}
		kv.mu.Unlock()

		// snapshot
		// if kv.maxraftstate != -1 && kv.persister.RaftStateSize() > kv.maxraftstate {
		// 	kv.saveSnap(apply.CommandIndex)
		// }
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) Killed() bool {
	return kv.rf.Killed()
}

// check if raft group can serve the shard now
func (kv *ShardKV) canServe(shardID int) Err {
	if kv.currentConfig.Shards[shardID] != kv.gid {
		return ErrWrongGroup
	}
	if kv.stateMachines[shardID] == nil || kv.stateMachines[shardID].Status != Serving {
		return CurUnvalable
	}
	return OK
}

func (kv *ShardKV) updateShardStatus(config *shardctrler.Config) {
	for shard, gid := range config.Shards {
		if kv.stateMachines[shard] == nil {
			kv.stateMachines[shard] = NewShard()
		}

		if gid == kv.gid { // if shard is assigned to this group
			if kv.currentConfig.Shards[shard] != kv.gid && kv.currentConfig.Num != 0 {
				kv.stateMachines[shard].Status = Pulling
			}

		} else if kv.currentConfig.Shards[shard] == kv.gid && kv.currentConfig.Num != 0 { // if shard is assigned to other group
			kv.stateMachines[shard].Status = BePulled
		}

	}
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(Command{})
	labgob.Register(shardctrler.Config{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.NotifyChan = make(map[int]chan CommandResponse)
	kv.stateMachines = make(map[int]*Shard)
	kv.retry = 3
	kv.lastConfig = shardctrler.Config{}
	kv.LastMsgID = make(map[int64]int)
	kv.currentConfig = shardctrler.Config{}

	kv.sc = shardctrler.MakeClerk(kv.ctrlers)

	go kv.applyLoop()
	go kv.Monitor(kv.queryConfig, ConfigureQueryInterval)

	return kv
}
