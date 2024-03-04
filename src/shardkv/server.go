package shardkv

import (
	"bytes"
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
	persister     *raft.Persister
	sc            *shardctrler.Clerk
	retry         int
	LastMsgID     map[int64]int // clientID -> last executed messageID

}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.RLock()
	shardID := key2shard(args.Key)
	shardStatus := kv.canServe(shardID)
	if shardStatus != OK {
		reply.Err = shardStatus
		reply.Value = ""
		kv.mu.RUnlock()
		return
	}
	kv.mu.RUnlock()

	op := Op{Operation: "Get", ClientID: args.ClientID, MsgID: args.MsgID, Key: args.Key}
	response := CommandResponse{}

	kv.Execute(NewOperationCommand(&op), &response)

	reply.Err = response.Err
	reply.Value = response.Value

}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.RLock()
	shardID := key2shard(args.Key)
	shardStatus := kv.canServe(shardID)
	if shardStatus != OK {
		reply.Err = shardStatus
		kv.mu.RUnlock()
		return
	}
	kv.mu.RUnlock()

	op := Op{Operation: args.Op, ClientID: args.ClientID, MsgID: args.MsgID, Key: args.Key, Value: args.Value}
	command := Command{Op: Operation, Data: op}
	response := CommandResponse{}

	kv.Execute(command, &response)

	reply.Err = response.Err
}

func (kv *ShardKV) GetShardsData(request *ShardOperationRequest, response *ShardOperationResponse) {
	// only pull shards from leader
	if _, isLeader := kv.rf.GetState(); !isLeader {
		response.Err = ErrWrongLeader
		return
	}
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	if kv.currentConfig.Num != request.ConfigNum {
		response.Err = ErrNotReady
		return
	}

	response.Shards = make(map[int]map[string]string)
	for _, shardID := range request.ShardIDs {
		if kv.stateMachines[shardID].Status != BePulled {
			response.Err = ErrOutDated
			return
		}
		response.Shards[shardID] = deepCopy(kv.stateMachines[shardID].KV)
	}

	response.LastMsgID = make(map[int64]int)
	for k, v := range kv.LastMsgID {
		response.LastMsgID[k] = v
	}

	response.ConfigNum, response.Err = request.ConfigNum, OK
}

func (kv *ShardKV) GCshards(request *ShardOperationRequest, response *CommandResponse) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		response.Err = ErrWrongLeader
		return
	}

	kv.mu.RLock()

	if kv.currentConfig.Num != request.ConfigNum {
		if kv.currentConfig.Num < request.ConfigNum {
			response.Err = ErrNotReady
		} else { // if currentConfig.Num > request.ConfigNum means old shard has been deleted
			response.Err = OK
		}
		kv.mu.RUnlock()
		return
	}

	kv.mu.RUnlock()

	kv.Execute(NewDeleteShardsCommand(request), response)

}

func deepCopy(oldshard map[string]string) map[string]string {
	newShard := make(map[string]string)
	for k, v := range oldshard {
		newShard[k] = v
	}
	return newShard
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

func (kv *ShardKV) getShardIDsByStatus(status ShardStatus) map[int][]int {
	gid2shardIDs := make(map[int][]int)
	shardIDs := make([]int, 0)
	// Debug(dServer, "G%d S%d curConfig %v,lastConfig %v", kv.gid, kv.me, kv.currentConfig.Shards, kv.lastConfig.Shards)
	for shard, shardptr := range kv.stateMachines {
		if shardptr.Status == status {
			Debug(dServer, "G%d S%d shard %d status %v", kv.gid, kv.me, shard, status)
			shardIDs = append(shardIDs, shard)
		}
	}

	for _, shardID := range shardIDs {
		// Debug(dServer, "G%d S%d gid %d shardID %d", kv.gid, kv.me, kv.lastConfig.Shards[shardID], shardID)
		gid2shardIDs[kv.lastConfig.Shards[shardID]] = append(gid2shardIDs[kv.lastConfig.Shards[shardID]], shardID)

	}
	// Debug(dServer, "G%d S%d gid2shardIDs %v", kv.gid, kv.me, gid2shardIDs)
	return gid2shardIDs
}

func (kv *ShardKV) saveSnap(index int) {
	// Debug(dSnap, "S%d save snapshot", kv.me)
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	// deepcopy stateMachines
	for k, v := range kv.stateMachines {
		e.Encode(k)
		e.Encode(*v)
	}
	e.Encode(kv.LastMsgID)
	e.Encode(kv.lastConfig)
	e.Encode(kv.currentConfig)
	data := w.Bytes()
	kv.rf.Snapshot(index, data)
}

func (kv *ShardKV) readSnapshot(data []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// Debug(dSnap, "S%d read snapshot", kv.me)
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	// var stateMachines map[int]*Shard
	stateMachines := make(map[int]*Shard)
	var lastMsgID map[int64]int
	var lastConfig shardctrler.Config
	var currentConfig shardctrler.Config

	for i := 0; i < shardctrler.NShards; i++ {
		var shardID int
		var shard Shard
		if d.Decode(&shardID) != nil || d.Decode(&shard) != nil {
			Debug(dLog, "S%d decode snapshot error", kv.me)
			return
		}
		stateMachines[shardID] = &shard
	}
	kv.stateMachines = stateMachines

	if d.Decode(&lastMsgID) != nil ||
		d.Decode(&lastConfig) != nil ||
		d.Decode(&currentConfig) != nil {
		Debug(dLog, "S%d decode snapshot error", kv.me)
	} else {
		kv.LastMsgID = lastMsgID
		kv.lastConfig = lastConfig
		kv.currentConfig = currentConfig
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
	labgob.Register(ShardOperationRequest{})
	labgob.Register(ShardOperationResponse{})

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
	kv.persister = persister
	kv.sc = shardctrler.MakeClerk(kv.ctrlers)
	kv.readSnapshot(persister.ReadSnapshot())

	go kv.applyLoop()
	go kv.Monitor(kv.queryConfig, ConfigureQueryInterval)
	go kv.Monitor(kv.migrationShard, MigrationShardInterval)

	return kv
}
