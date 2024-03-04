package shardkv

import (
	"MIT6824/shardctrler"
	"sync"
	"time"
)

func (kv *ShardKV) Monitor(funcPtr func(), interval time.Duration) {
	for {
		// only leader need to run go routine
		if _, isLeader := kv.rf.GetState(); isLeader  {
			if kv.Killed() {
				return
			}
			funcPtr()
		}
		time.Sleep(interval)
	}

}

func (kv *ShardKV) queryConfig() {
	canPerformNextConfig := true
	kv.mu.RLock()
	for _, shard := range kv.stateMachines {
		if shard.Status != Serving {
			canPerformNextConfig = false
			// DPrintf("{Node %v}{Group %v} will not try to fetch latest configuration because shards status are %v when currentConfig is %v", kv.rf.Me(), kv.gid, kv.getShardStatus(), kv.currentConfig)
			break
		}
	}
	currentConfigNum := kv.currentConfig.Num
	kv.mu.RUnlock()
	if canPerformNextConfig {
		nextConfig := kv.sc.Query(currentConfigNum + 1)
		// nextConfig := kv.sc.Query(-1)

		if nextConfig.Num == currentConfigNum+1 {
			// Debug(dInfo, "G%d S%d updates currentConfig from %d to %d,config is %v", kv.gid, kv.me, kv.currentConfig.Num, nextConfig.Num, nextConfig)
			kv.Execute(NewConfigurationCommand(&nextConfig), &CommandResponse{})
		}
	}
}

func (kv *ShardKV) migrationShard() {
	kv.mu.RLock()
	gid2shardIDs := kv.getShardIDsByStatus(Pulling)
	var wg sync.WaitGroup
	for gid, shardIDs := range gid2shardIDs {
		Debug(dServer, "G%d S%d pull shards %v from gid:G%d", kv.gid, kv.me, shardIDs, gid)
		wg.Add(1)
		go func(servers []string, configNum int, shardIDs []int) {
			defer wg.Done()
			pullTaskRequest := ShardOperationRequest{configNum, shardIDs}
			for _, server := range servers {
				var pullTaskResponse ShardOperationResponse
				srv := kv.make_end(server)
				if srv.Call("ShardKV.GetShardsData", &pullTaskRequest, &pullTaskResponse) && pullTaskResponse.Err == OK {
					var response CommandResponse
					kv.Execute(NewInsertShardsCommand(&pullTaskResponse), &response)
					if response.Err == OK {
						// 成功之后是否应该通知对方删除数据
						go kv.notifyGC(servers, &pullTaskRequest)
						return
					}
				}

			}
		}(kv.lastConfig.Groups[gid], kv.currentConfig.Num, shardIDs)
	}
	kv.mu.RUnlock()
	wg.Wait()
}

func (kv *ShardKV) notifyGC(servers []string, req *ShardOperationRequest) {
	index := 0
	for {
		server := servers[index]
		srv := kv.make_end(server)
		for i := 0; i < kv.retry; i++ {
			var gcResponse CommandResponse
			ok := srv.Call("ShardKV.GCshards", req, &gcResponse)
			if ok {
				if gcResponse.Err == OK {
					return
				}
				if gcResponse.Err == ErrWrongLeader {
					break
				}
			}
		}
		index = (index + 1) % len(servers)
	}
}

func (kv *ShardKV) applyLoop() {
	for apply := range kv.applyCh {
		if kv.Killed() {
			return
		}

		if apply.SnapshotValid {
			// Debug(dSnap, "S%d restore snapshot", kv.me)
			kv.readSnapshot(apply.Snapshot)
			continue
		}

		if !apply.CommandValid {
			continue
		}

		command := apply.Command.(Command)
		kv.mu.Lock()

		var response *CommandResponse

		switch command.Op {
		case Operation:
			operation := command.Data.(Op)
			response = kv.applyOperation(&operation)
		case Configuration:
			config := command.Data.(shardctrler.Config)
			response = kv.applyConfiguration(&config)
		case InsertShards:
			insertShardsResponse := command.Data.(ShardOperationResponse)
			response = kv.applyInsertShards(&insertShardsResponse)
		case DeleteShards:
			deleteShardsRequest := command.Data.(ShardOperationRequest)
			response = kv.applyDeleteShards(&deleteShardsRequest)
		}

		term, isLeader := kv.rf.GetState()

		// notify rpc handler
		if _, ok := kv.NotifyChan[apply.CommandIndex]; ok && isLeader && term == apply.CommandTerm {
			kv.NotifyChan[apply.CommandIndex] <- *response
		}
		kv.mu.Unlock()

		// snapshot
		if kv.maxraftstate != -1 && kv.persister.RaftStateSize() > kv.maxraftstate {
			kv.saveSnap(apply.CommandIndex)
		}
	}
}