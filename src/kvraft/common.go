package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrExecuted    = "ErrExecuted"
	ErrNetwork     = "ErrNetwork"
	ErrTimeout     = "ErrTimeout"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Id    int64
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err      Err
}

type GetArgs struct {
	Id  int64
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err      Err
	Value    string
}
