package shardkv

import (
	"MIT6824/shardctrler"
)

func (kv *ShardKV) applyConfiguration(nextConfig *shardctrler.Config) *CommandResponse {
	if nextConfig.Num == kv.currentConfig.Num+1 {
		kv.updateShardStatus(nextConfig)
		kv.lastConfig = kv.currentConfig
		kv.currentConfig = *nextConfig
		Debug(dInfo, "G%d S%d .updates currentConfig from %d to %d", kv.gid, kv.me, kv.lastConfig.Num, kv.currentConfig.Num)
		return &CommandResponse{OK, ""}
	}

	return &CommandResponse{ErrOutDated, ""}
}

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

func (kv *ShardKV) applyInsertShards(shardsInfo *ShardOperationResponse) *CommandResponse {
	if shardsInfo.ConfigNum == kv.currentConfig.Num {
		for shardId, shardData := range shardsInfo.Shards {
			if kv.stateMachines[shardId].Status == Pulling {
				kv.stateMachines[shardId].KV = deepCopy(shardData)
				kv.stateMachines[shardId].Status = Serving
			}
		}

		for clientID, msgID := range shardsInfo.LastMsgID {
			if lastmsgID, ok := kv.LastMsgID[clientID]; !ok || msgID > lastmsgID {
				kv.LastMsgID[clientID] = msgID
			}
		}

		return &CommandResponse{OK, ""}
	}
	return &CommandResponse{ErrOutDated, ""}
}

func (kv *ShardKV) applyDeleteShards(shardsInfo *ShardOperationRequest) *CommandResponse {
	if shardsInfo.ConfigNum == kv.currentConfig.Num {
		for _, shardId := range shardsInfo.ShardIDs {
			shard := kv.stateMachines[shardId]
			if shard.Status == BePulled {
				kv.stateMachines[shardId] = NewShard()
			}
		}
		return &CommandResponse{OK, ""}
	}
	return &CommandResponse{OK, ""}
}
