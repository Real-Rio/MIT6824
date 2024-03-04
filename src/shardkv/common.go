package shardkv

import "time"

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	CurUnvalable   = "CurUnvalable"
	TimeOut        = "TimeOut"
	ErrOutDated    = "ErrOutDated"
	ErrNotReady    = "ErrNotReady"
)

// go routine interval
const (
	ConfigureQueryInterval = 90 * time.Millisecond
	MigrationShardInterval = 50 * time.Millisecond
	WaitingInterval        = 70 * time.Millisecond // wait for config to catch up
)

type Err string

type ShardStatus uint8

const (
	Serving ShardStatus = iota
	Pulling
	BePulled
	// NeedGC // shard need to be garbage collected
)

type CommandResponse struct {
	Err   Err
	Value string
}

// Put or Append
type PutAppendArgs struct {
	Key      string
	Value    string
	Op       string // "Put" or "Append"
	ClientID int64
	MsgID    int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key      string
	ClientID int64
	MsgID    int
}

type GetReply struct {
	Err   Err
	Value string
}

type ShardOperationRequest struct {
	ConfigNum int
	ShardIDs  []int
}

type ShardOperationResponse struct {
	Shards    map[int]map[string]string
	ConfigNum int
	Err       Err
	LastMsgID map[int64]int
}
