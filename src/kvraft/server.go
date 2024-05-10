package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

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
	// index        int
	// term         int

	// Your definitions here.
	db      map[string]string
	chanMap *KVChanMap
}

func getTimeout() time.Duration {
	return time.Duration(800) * time.Millisecond
}

func (kv *KVServer) putAppendDB(oper, key, val string) {
	if oper == "Put" {
		kv.db[key] = val
	} else if oper == "Append" {
		kv.db[key] += val
	}
}

func (kv *KVServer) getDB(key string) string {
	if val, ok := kv.db[key]; ok {
		return val
	}

	return ""
}

func monitor(ch chan Op) Op {
	select {
	case op := <-ch:
		return op
	case <-time.After(getTimeout()):
		return Op{}
	}
}

func opsEqual(op, commitedOp Op) bool {
	return (op.Id == commitedOp.Id &&
		op.Method == commitedOp.Method &&
		op.Key == commitedOp.Key &&
		op.Value == commitedOp.Value)
}

func (kv *KVServer) checkArgs(id int64, key string) Err {
	var err Err
	err = OK

	if key == "" {
		err = ErrNoKey
	} else if kv.chanMap.contains(id) {
		err = ErrExecuted
	}

	return err
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	DPrintf("Get called")
	// Your code here.
	reply.Err = ErrWrongLeader

	if err := kv.checkArgs(args.Id, args.Key); err != OK {
		reply.Err = err
		return
	}

	op := Op{
		Id:     args.Id,
		Method: "Get",
		Key:    args.Key,
	}

	opChan := kv.chanMap.add(args.Id)

	_, _, isLeader := kv.rf.Start(op)

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	commitedOp := monitor(opChan)
	if opsEqual(op, commitedOp) {
		reply.Err = OK
		reply.Value = kv.getDB(op.Key)
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	DPrintf("PutAppend called")
	// Your code here.
	reply.Err = ErrWrongLeader

	if err := kv.checkArgs(args.Id, args.Key); err != OK {
		reply.Err = err
		return
	}

	op := Op{
		Id:     args.Id,
		Method: args.Op,
		Key:    args.Key,
		Value:  args.Value,
	}

	opChan := kv.chanMap.add(args.Id)

	_, _, isLeader := kv.rf.Start(op)

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	commitedOp := monitor(opChan)
	if opsEqual(op, commitedOp) {
		reply.Err = OK
	}
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

		op := m.Command.(Op)

		// do operation
		kv.putAppendDB(op.Method, op.Key, op.Value)

		// send to chan or ignore
		if ok := kv.chanMap.contains(op.Id); ok {
			respCh := kv.chanMap.get(op.Id)
			respCh <- op
		}
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
	kv.db = make(map[string]string)
	kv.chanMap = makeKVChanMap()
	go kv.applyChLoop()

	return kv
}
