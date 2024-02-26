package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"MIT6824/labrpc"
)

type Clerk struct {
	servers  []*labrpc.ClientEnd
	clientID int64
	msgID    int
	retry    int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.clientID = nrand()
	ck.msgID = 1
	ck.retry = 3
	return ck
}

func (ck *Clerk) Query(num int) Config {
	Debug(dClient, "client initiates Query num:%d", num)
	args := &QueryArgs{}
	// Your code here.
	args.Num = num
	for {
		// try each known server.
		for _, srv := range ck.servers {
			for i := 0; i < ck.retry; i++ {
				var reply QueryReply
				ok := srv.Call("ShardCtrler.Query", args, &reply)
				if ok {
					if reply.WrongLeader {
						break
					} else if reply.Err == OK {
						return reply.Config
					}
				}
				time.Sleep(100 * time.Millisecond)
			}
		}
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	Debug(dClient, "client initiates Join servers:%v", servers)
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers
	args.ClientID = ck.clientID
	args.MsgID = ck.msgID

	for {
		// try each known server.
		for _, srv := range ck.servers {
			for i := 0; i < ck.retry; i++ {
				var reply JoinReply
				ok := srv.Call("ShardCtrler.Join", args, &reply)
				if ok {
					if reply.WrongLeader {
						break
					} else if reply.Err == OK {
						ck.msgID++
						return
					}
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	Debug(dClient, "client initiates Leave gids:%v", gids)
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	args.ClientID = ck.clientID
	args.MsgID = ck.msgID

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardCtrler.Leave", args, &reply)
			if ok && !reply.WrongLeader {
				ck.msgID++
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	Debug(dClient, "client initiates Move shard:%d gid:%d", shard, gid)
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	args.ClientID = ck.clientID
	args.MsgID = ck.msgID

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			if ok && !reply.WrongLeader {
				ck.msgID++
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
