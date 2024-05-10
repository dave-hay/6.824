package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	mu       sync.RWMutex
	leaderId int
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
	// You'll have to add code here.
	return ck
}

func (ck *Clerk) getLeaderId() int {
	ck.mu.RLock()
	defer ck.mu.RUnlock()
	return ck.leaderId
}

func (ck *Clerk) setLeaderId(id int) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.leaderId = id
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
	// You will have to modify this function.
	if key == "" {
		return ""
	}

	leaderId := ck.getLeaderId()

	id := nrand()
	args := &GetArgs{Id: id, Key: key}

	for {
		reply := &GetReply{}
		ok := ck.servers[leaderId].Call("KVServer.Get", args, reply)

		if ok && reply.Err == OK {
			ck.setLeaderId(leaderId)
			return reply.Value
		}

		leaderId = (leaderId + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
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

	if key == "" {
		return
	}

	id := nrand()
	args := &PutAppendArgs{Id: id, Key: key, Value: value, Op: op}

	leaderId := ck.getLeaderId()

	for {
		reply := &PutAppendReply{}
		ok := ck.servers[leaderId].Call("KVServer.PutAppend", args, reply)

		if ok && reply.Err == OK {
			ck.setLeaderId(leaderId)
			return
		}

		leaderId = (leaderId + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
