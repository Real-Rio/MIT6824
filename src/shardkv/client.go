package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"MIT6824/labrpc"
	"MIT6824/shardctrler"
)

// which shard is a key in?
// please use this function,
// and please do not change it.
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	clientID  int64
	msgID     int
	curLeader int
	retry     int
}

// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	ck.clientID = nrand()
	ck.msgID = 1
	ck.retry = 3
	ck.curLeader = 0
	ck.config = ck.sm.Query(-1)
	// You'll have to add code here.
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
func (ck *Clerk) Get(key string) string {
	args := GetArgs{}
	args.Key = key
	args.MsgID = ck.msgID
	args.ClientID = ck.clientID
	wrongGroup := false

	for {
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)

		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {

			for {
				si := ck.curLeader
				wrongGroup = false
				srv := ck.make_end(servers[si])
				for i := 0; i < ck.retry; i++ {
					var reply GetReply
					ok := srv.Call("ShardKV.Get", &args, &reply)

					if ok {
						if reply.Err == OK || reply.Err == ErrNoKey {
							ck.msgID++
							return reply.Value
						}
						if reply.Err == ErrWrongLeader {
							break
						}
						if reply.Err == ErrWrongGroup {
							wrongGroup = true
							break
						}
					}
					time.Sleep(100 * time.Millisecond)

				}
				if wrongGroup {
					ck.curLeader = 0
					break
				}
				ck.curLeader = (ck.curLeader + 1) % len(servers)
			}
			// }
		}
		time.Sleep(100 * time.Millisecond)

	}

}

// shared by Put and Append.
// You will have to modify this function.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.Op = op
	args.MsgID = ck.msgID
	args.ClientID = ck.clientID
	wrongGroup := false

	for {
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)

		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			// for si := 0; si < len(servers); si++ {
			for {
				si := ck.curLeader
				wrongGroup = false

				srv := ck.make_end(servers[si])
				for i := 0; i < ck.retry; i++ {
					var reply PutAppendReply
					ok := srv.Call("ShardKV.PutAppend", &args, &reply)
					if ok {
						if reply.Err == OK || reply.Err == ErrNoKey {
							ck.msgID++
							return
						}
						if reply.Err == ErrWrongLeader {
							break
						}
						if reply.Err == ErrWrongGroup {
							wrongGroup = true
							break
						}
					}
					// ... not ok, or ErrWrongLeader
				}
				if wrongGroup {
					ck.curLeader = 0
					break
				}
				// }
				ck.curLeader = (ck.curLeader + 1) % len(servers)
			}
		}
		time.Sleep(100 * time.Millisecond)

	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
