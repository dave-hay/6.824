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
	id := nrand()
	args := &GetArgs{Id: id, Key: key}
	reply := &GetReply{}

	for {
		leader := ck.getLeaderId()
		ck.sendGet(leader, args, reply)
		switch reply.Err {
		case OK:
			return reply.Value
		case ErrNoKey:
			args.Key = key
		case ErrWrongLeader:
			ck.setLeaderId(reply.LeaderId)
		case ErrExecuted:
			return ""
		}
		time.Sleep(10 * time.Millisecond)
	}

}

// want to know if success or if it needs to be called again
func (ck *Clerk) sendGet(server int, args *GetArgs, reply *GetReply) {
	if ok := ck.servers[server].Call("KVServer.Get", args, reply); !ok {
		reply.Err = ErrNetwork
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
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
