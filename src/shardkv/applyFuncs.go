package shardkv

import (
	"MIT6824/shardctrler"
)

func (kv *ShardKV) applyConfiguration(nextConfig *shardctrler.Config) *CommandResponse {
	if nextConfig.Num > kv.currentConfig.Num {
		// DPrintf("{Node %v}{Group %v} updates currentConfig from %v to %v", kv.rf.Me(), kv.gid, kv.currentConfig, nextConfig)
		kv.updateShardStatus(nextConfig)
		kv.lastConfig = kv.currentConfig
		kv.currentConfig = *nextConfig
		return &CommandResponse{OK, ""}
	}
	// DPrintf("{Node %v}{Group %v} rejects outdated config %v when currentConfig is %v", kv.rf.Me(), kv.gid, nextConfig, kv.currentConfig)
	return &CommandResponse{ErrOutDated, ""}
}

// TODO:应该加锁吗？如果修改数据的时候 config 变了怎么办
func (kv *ShardKV) applyOperation(op *Op) *CommandResponse {
	response := &CommandResponse{Err: OK}
	shardID := key2shard(op.Key)
	status := kv.canServe(shardID)
	if status == OK {
		switch op.Operation {
		case "Get":
			response.Value, response.Err = kv.stateMachines[shardID].Get(op.Key)

			if op.MsgID > kv.LastMsgID[op.ClientID] {
				kv.LastMsgID[op.ClientID] = op.MsgID
			}
			return response
		case "Put":
			if op.MsgID > kv.LastMsgID[op.ClientID] {
				response.Err = kv.stateMachines[shardID].Put(op.Key, op.Value)
				kv.LastMsgID[op.ClientID] = op.MsgID
			}
			return response
		case "Append":
			if op.MsgID > kv.LastMsgID[op.ClientID] {
				response.Err = kv.stateMachines[shardID].Append(op.Key, op.Value)
				kv.LastMsgID[op.ClientID] = op.MsgID
			}
			return response
		}
	}
	return &CommandResponse{status, ""}
}
