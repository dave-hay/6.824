# Raft

`Start()`

Service (e.g. a k/v server) using RAFT wants to start agreement on the next command to be appended to Raft's log.
If this server isn't the leader, returns false.
If server start the agreement and return immediately.

No guarantee that this command will ever be committed to the Raft log, since the leader may fail or lose an election.

Even if the Raft instance has been killed, this function should return gracefully.

**Return**: `index, term, isLeader`

- `index`: index that the command (if committed).
- `term`: current term.
- `isLeader`: true if server believes it is leader.

```go
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	return index, term, isLeader
}
```

`Call()`:

Simulates lossy network, servers may be unreachable or req/res may be lost.

- sends RPC and waits for a reply. may be delayed
- returns `true`: If a reply arrives within a timeout interval.
- returns `false`: if doesn't receive reply in time. Due to dead server/lost req/res.

`RequestVote` RPC

- server is the index of the target server in `rf.peers[]`
- expects RPC arguments in args.
- fills in \*reply with RPC reply, so caller should pass &reply.

```go
func (rf *Raft) sendRequestVote(server, args, reply) bool {
    ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
    return ok
}
```
