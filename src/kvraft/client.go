package kvraft

import (
	"MIT6824/labrpc"
	"crypto/rand"
	"math/big"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	curLeader int
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
func (ck *Clerk) Get(key string) string {
	Debug(dClient, "client initiates Get key:%s", key)
	args := GetArgs{Key: key, ClientID: ck.clientID, MsgID: ck.messageID}
	ck.messageID++

	for {
		Debug(dWarn, "get curserver %d id:%d", ck.curLeader, args.MsgID)

		reply := GetReply{}
		ok := ck.servers[ck.curLeader].Call("KVServer.Get", &args, &reply)
		if ok {
			if reply.Err == OK {
				Debug(dClient, "Get key:%s value:%s success id:%d", key, reply.Value, args.MsgID)
				return reply.Value
			}
			if reply.Err == ErrNoKey {
				return ""
			}
		}

		ck.curLeader = (ck.curLeader + 1) % len(ck.servers)
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
	args := PutAppendArgs{Key: key, Value: value, Op: op, ClientID: ck.clientID, MsgID: ck.messageID}
	ck.messageID++

	for {
		Debug(dWarn, "PutAppend curserver %d id:%d", ck.curLeader, args.MsgID)
		reply := PutAppendReply{}
		ok := ck.servers[ck.curLeader].Call("KVServer.PutAppend", &args, &reply)
		if ok && reply.Err == OK {
			Debug(dClient, "PutAppend success key:%s value:%s id:%d", args.Key, args.Value, args.MsgID)
			return

		}
		ck.curLeader = (ck.curLeader + 1) % len(ck.servers)

	}
}

func (ck *Clerk) Put(key string, value string) {
	Debug(dClient, "client initiates Put key:%s value:%s", key, value)
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	Debug(dClient, "client initiates Append key:%s value:%s", key, value)
	ck.PutAppend(key, value, "Append")
}
