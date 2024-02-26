package shardctrler

import (
	"sort"
	"sync"
	"time"

	"MIT6824/labgob"
	"MIT6824/labrpc"
	"MIT6824/raft"
)

type ShardCtrler struct {
	mu      sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	NotifyChan map[int]chan interface{}
	LastMsgID  map[int64]int // clientID -> last executed msgID
	configs    []Config      // indexed by config num
}

type Op struct {
	Operation string // "Join" "Leave" "Move" "Query"
	ClientID  int64
	MsgID     int
	Args      interface{}
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	command := Op{
		Operation: "Join",
		ClientID:  args.ClientID,
		MsgID:     args.MsgID,
		Args:      *args,
	}

	wrongLeader, err := sc.sendMsgToRaft(&command)
	reply.WrongLeader = wrongLeader
	reply.Err = err
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	command := Op{
		Operation: "Leave",
		ClientID:  args.ClientID,
		MsgID:     args.MsgID,
		Args:      *args,
	}
	wrongLeader, err := sc.sendMsgToRaft(&command)
	reply.WrongLeader = wrongLeader
	reply.Err = err
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	command := Op{
		Operation: "Move",
		ClientID:  args.ClientID,
		MsgID:     args.MsgID,
		Args:      *args,
	}
	wrongLeader, err := sc.sendMsgToRaft(&command)
	reply.WrongLeader = wrongLeader
	reply.Err = err
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// check if leader

	command := Op{
		Operation: "Query",
		Args:      *args,
	}
	// TODO:读请求放到 raft 中
	wrongLeader, err := sc.sendMsgToRaft(&command)
	reply.WrongLeader = wrongLeader
	reply.Err = err

	if err == OK && !wrongLeader {
		queryNum := args.Num
		// if queryNum is -1 or larger than latest num, return the latest config
		if queryNum < 0 || queryNum >= len(sc.configs) {
			queryNum = len(sc.configs) - 1
		}
		reply.Config = sc.configs[queryNum]
	}


	Debug(dServer, "S%d Query config %v", sc.me, reply.Config)
}

func (sc *ShardCtrler) applyJoin(groups map[int][]string) {
	lastConfig := sc.configs[len(sc.configs)-1]
	newConfig := Config{len(sc.configs), lastConfig.Shards, deepCopy(lastConfig.Groups)}

	for gid, servers := range groups {
		if _, ok := newConfig.Groups[gid]; !ok {
			newServers := make([]string, len(servers))
			copy(newServers, servers)
			newConfig.Groups[gid] = newServers
		}
	}

	// gid-> shard
	g2s := groupToShards(newConfig)

	// rebalance
	// make gid with most shards
	for {
		s, t := getMaxNumShardByGid(g2s), getMinNumShardByGid(g2s)
		if s != 0 && len(g2s[s])-len(g2s[t]) <= 1 {
			break
		}
		g2s[t] = append(g2s[t], g2s[s][0])
		g2s[s] = g2s[s][1:]
	}

	var newShards [NShards]int
	for gid, shards := range g2s {
		for _, shardId := range shards {
			newShards[shardId] = gid
		}
	}
	newConfig.Shards = newShards
	sc.configs = append(sc.configs, newConfig)
}

func (sc *ShardCtrler) applyLeave(gids []int) {
	lastConfig := sc.configs[len(sc.configs)-1]
	newConfig := Config{len(sc.configs), lastConfig.Shards, deepCopy(lastConfig.Groups)}

	g2s := groupToShards(newConfig)

	noUsedShards := make([]int, 0)
	for _, gid := range gids {
		delete(newConfig.Groups, gid)

		if shards, ok := g2s[gid]; ok {
			noUsedShards = append(noUsedShards, shards...)
			delete(g2s, gid)
		}
	}

	var newShards [NShards]int
	if len(newConfig.Groups) > 0 {
		for _, shardId := range noUsedShards {
			t := getMinNumShardByGid(g2s)
			g2s[t] = append(g2s[t], shardId)
		}

		for gid, shards := range g2s {
			for _, shardId := range shards {
				newShards[shardId] = gid
			}
		}
	}
	newConfig.Shards = newShards
	sc.configs = append(sc.configs, newConfig)
}

func (sc *ShardCtrler) applyMove(shard int, gid int) {
	lastConfig := sc.configs[len(sc.configs)-1]
	newConfig := Config{len(sc.configs), lastConfig.Shards, deepCopy(lastConfig.Groups)}
	newConfig.Shards[shard] = gid
	sc.configs = append(sc.configs, newConfig)
}

func (sc *ShardCtrler) sendMsgToRaft(command *Op) (bool, Err) {
	index, _, isLeader := sc.rf.Start(*command)
	if !isLeader {
		return true, ""
	}

	sc.mu.Lock()
	if sc.NotifyChan[index] == nil {
		sc.NotifyChan[index] = make(chan interface{}, 1)
	}
	notifyChan := sc.NotifyChan[index]
	sc.mu.Unlock()

	var err Err
	select {
	case <-notifyChan:
		err = OK
	case <-time.After(500 * time.Millisecond):
		err = TimeOut
	}

	sc.mu.Lock()
	close(sc.NotifyChan[index])
	delete(sc.NotifyChan, index)
	sc.mu.Unlock()
	return false, err
}

func (sc *ShardCtrler) applyLoop() {
	for apply := range sc.applyCh {
		Debug(dInfo, "S%d applyLoop", sc.me)
		op := apply.Command.(Op)
		clientID := op.ClientID
		msgID := op.MsgID

		sc.mu.Lock()
		if op.Operation == "Join" && msgID > sc.LastMsgID[clientID] {
			sc.LastMsgID[clientID] = msgID
			sc.applyJoin(op.Args.(JoinArgs).Servers)
		} else if op.Operation == "Leave" && msgID > sc.LastMsgID[clientID] {
			sc.LastMsgID[clientID] = msgID
			sc.applyLeave(op.Args.(LeaveArgs).GIDs)
		} else if op.Operation == "Move" && msgID > sc.LastMsgID[clientID] {
			sc.LastMsgID[clientID] = msgID
			sc.applyMove(op.Args.(MoveArgs).Shard, op.Args.(MoveArgs).GID)
		}

		term, isLeader := sc.rf.GetState()

		// notify rpc handler
		if _, ok := sc.NotifyChan[apply.CommandIndex]; ok && isLeader && term == apply.CommandTerm {
			sc.NotifyChan[apply.CommandIndex] <- nil
		}
		sc.mu.Unlock()
	}
}

func groupToShards(config Config) map[int][]int {
	g2s := make(map[int][]int)
	for gid := range config.Groups {
		g2s[gid] = make([]int, 0)
	}
	for shardId, gid := range config.Shards {
		g2s[gid] = append(g2s[gid], shardId)
	}
	return g2s
}

func deepCopy(groups map[int][]string) map[int][]string {
	newGroups := make(map[int][]string)
	for gid, servers := range groups {
		newServers := make([]string, len(servers))
		copy(newServers, servers)
		newGroups[gid] = newServers
	}
	return newGroups
}

func getMinNumShardByGid(g2s map[int][]int) int {
	// 不固定顺序的话，可能会导致两次的config不同
	gids := make([]int, 0)
	for key := range g2s {
		gids = append(gids, key)
	}

	sort.Ints(gids)

	min, index := NShards+1, -1
	for _, gid := range gids {
		if gid != 0 && len(g2s[gid]) < min {
			min = len(g2s[gid])
			index = gid
		}
	}
	return index
}

func getMaxNumShardByGid(g2s map[int][]int) int {
	// GID = 0 是无效配置，一开始所有分片分配给GID=0
	if shards, ok := g2s[0]; ok && len(shards) > 0 {
		return 0
	}

	gids := make([]int, 0)
	for key := range g2s {
		gids = append(gids, key)
	}

	sort.Ints(gids)

	max, index := -1, -1
	for _, gid := range gids {
		if len(g2s[gid]) > max {
			max = len(g2s[gid])
			index = gid
		}
	}
	return index
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(QueryArgs{})
	labgob.Register(Op{})

	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}
	sc.configs[0].Shards = [NShards]int{}

	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	sc.NotifyChan = make(map[int]chan interface{})
	sc.LastMsgID = make(map[int64]int)
	go sc.applyLoop()
	return sc
}
