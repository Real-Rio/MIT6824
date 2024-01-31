package kvraft

import (
	"crypto/rand"
	"math/big"

	"MIT6824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	curLeader int
	retry     int
	clientID  int64
	messageID int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	Debug(dClient, "Clerk restart")
	ck := new(Clerk)
	ck.servers = servers
	ck.retry = 3
	ck.clientID = nrand()
	ck.messageID = 1
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
// TODO:需要知道谁是当前的 leader
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	Debug(dClient, "Get key:%s", key)

	serverID := ck.curLeader
	retry := ck.retry
	args := GetArgs{Key: key, ClientID: ck.clientID, MsgID: ck.messageID}
	ck.messageID++
	for {
		Debug(dWarn, "get curserver %d", serverID)

		for i := 0; i < retry; i++ {
			reply := GetReply{}
			ok := ck.servers[serverID].Call("KVServer.Get", &args, &reply)
			if ok {
				if reply.Err == OK {
					Debug(dClient, "Get key:%s value:%s success", key, reply.Value)
					return reply.Value
				}
				if reply.Err == ErrNoKey {
					return ""
				}
				if reply.Err == ErrWrongLeader {
					break
				}
			}

		}
		serverID = (serverID + 1) % len(ck.servers)
		ck.curLeader = serverID
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	serverID := ck.curLeader
	retry := ck.retry
	args := PutAppendArgs{Key: key, Value: value, Op: op, ClientID: ck.clientID, MsgID: ck.messageID}
	ck.messageID++
	for {
		Debug(dWarn, "PutAppend curserver %d", serverID)
		for i := 0; i < retry; i++ {
			reply := PutAppendReply{}
			Debug(dClient, "PutAppend key:%s value:%s ID:%d", key, value, args.MsgID)
			ok := ck.servers[serverID].Call("KVServer.PutAppend", &args, &reply)
			if ok {
				if reply.Err == OK {
					Debug(dClient, "PutAppend success", key, value, op)
					return
				}
				if reply.Err == ErrWrongLeader {
					break
				}
			}

		}
		serverID = (serverID + 1) % len(ck.servers)
		ck.curLeader = serverID
	}
}

func (ck *Clerk) Put(key string, value string) {
	Debug(dClient, "Put key:%s value:%s", key, value)
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	Debug(dClient, "Append key:%s value:%s", key, value)
	ck.PutAppend(key, value, "Append")
}
