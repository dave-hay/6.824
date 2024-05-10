package kvraft

import (
	"log"
	"sync"
	"sync/atomic"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	Id     int64
	Method string
	Key    string
	Value  string
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

// so raft can get ID
func (op *Op) GetId() int64 {
	return op.Id
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	db      *KVMap
	chanMap *KVChanMap
}

func (kv *KVServer) getTimeout() time.Duration {
	return time.Duration(800) * time.Millisecond
}

func (kv *KVServer) checkArgs(args *GetArgs, reply *GetReply) {
	if args.Key == "" {
		reply.Err = ErrNoKey
		return
	}

	if kv.chanMap.contains(args.Id) {
		reply.Err = ErrExecuted
		return
	}

	leaderId := kv.rf.GetLeaderId()
	if kv.me != leaderId {
		reply.Err = ErrWrongLeader
		reply.LeaderId = leaderId
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	DPrintf("Get called")
	// Your code here.
	if args.Key == "" {
		reply.Err = ErrNoKey
		return
	}

	op := Op{
		Id:     args.Id,
		Method: "Get",
		Key:    args.Key,
	}

	if kv.chanMap.contains(args.Id) {
		reply.Err = ErrExecuted
		return
	}

	entry := kv.chanMap.add(args.Id)
	index, term, isLeader := kv.rf.Start(op)

	if index == -1 && term == -1 && !isLeader {
		// can optimize by not deleting and having check on add
		kv.chanMap.del(args.Id)
		reply.Err = ErrWrongLeader
		reply.LeaderId = kv.rf.GetLeaderId()
		return
	}

	// TODO: add timeout

	// wait until raft sends applyCh
	<-entry.ch
	reply.Value = kv.db.get(args.Key)
	kv.chanMap.del(args.Id)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	DPrintf("PutAppend called")
	// Your code here.
	if args.Key == "" {
		reply.Err = ErrNoKey
		return
	}

	op := Op{
		Id:     args.Id,
		Method: args.Op,
		Key:    args.Key,
		Value:  args.Value,
	}

	if kv.chanMap.contains(args.Id) {
		reply.Err = ErrExecuted
		return
	}

	entry := kv.chanMap.add(args.Id)
	index, term, isLeader := kv.rf.Start(op)

	if index == -1 && term == -1 && !isLeader {
		DPrintf("PutAppend not leader")
		// can optimize by not deleting and having check on add
		kv.chanMap.del(args.Id)
		reply.Err = ErrWrongLeader
		reply.LeaderId = max(kv.rf.GetLeaderId(), 0)
		return
	}

	// TODO: add timeout

	DPrintf("PutAppend waiting on chan")
	<-entry.ch

	if args.Op == "Put" {
		kv.db.put(args.Key, args.Value)
	} else {
		kv.db.append(args.Key, args.Value)
	}
	kv.chanMap.del(args.Id)
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) applyChLoop() {
	for m := range kv.applyCh {
		DPrintf("PutAppend recieved applyCh")
		if !m.CommandValid {
			continue
		}
		resp := kv.chanMap.get(m.Id)
		resp.ch <- true
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.db = makeKVMap()
	kv.chanMap = makeKVChanMap()
	go kv.applyChLoop()

	return kv
}
