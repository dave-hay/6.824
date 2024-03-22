# Raft

The labrpc package simulates a lossy network, in which servers may be unreachable, and in which requests and replies may be lost. Call() sends a request and waits for a reply. If a reply arrives within a timeout interval, Call() returns true; otherwise Call() returns false. Thus Call() may not return for a while. A false return can be caused by a dead server, a live server that can't be reached, a lost request, or a lost reply.

Call() is guaranteed to return (perhaps after a delay) _except_ if the
handler function on the server side does not return. Thus there
is no need to implement your own timeouts around Call().

look at the comments in 6.824/labrpc/labrpc.go for more details.
