package shardkv

import "time"

func (kv *ShardKV) Monitor(funcPtr func(), interval time.Duration) {
	for {
		// only leader need to run go routine
		if _, isLeader := kv.rf.GetState(); isLeader {
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
		// nextConfig := kv.sc.Query(currentConfigNum + 1)
		nextConfig := kv.sc.Query(-1)

		if nextConfig.Num > currentConfigNum {
			Debug(dInfo, "S%d updates currentConfig from %d to %d", kv.me, kv.currentConfig.Num, nextConfig.Num)
			// DPrintf("{Node %v}{Group %v} fetches latest configuration %v when currentConfigNum is %v", kv.rf.Me(), kv.gid, nextConfig, currentConfigNum)
			kv.Execute(NewConfigurationCommand(&nextConfig), &CommandResponse{})
		}
	}
}
