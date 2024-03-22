# Raft

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
