package raft

import (
	"log"
	"sync"
)

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry
	LastLogTerm  int // term of candidate’s last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

	if args.CandidateId == rf.me {
		rf.mu.Lock()

		rf.VoteFor = args.CandidateId
		reply.Term = rf.currentTerm
		reply.VoteGranted = true

		rf.mu.Unlock()
		return
	}

	rf.mu.Lock()
	currentTerm := rf.currentTerm
	state := rf.state
	rf.mu.Unlock()

	reply.Term = currentTerm

	// Your code here (2A, 2B).
	if args.Term <= currentTerm {
		rf.mu.Lock()
		rf.VoteFor = VoteForNull
		rf.mu.Unlock()
		DPrintf("%s receive invalid rpc.Term: %d", rf, args.Term)
		reply.VoteGranted = false
		return
	}

	rf.resetTimerCh <- struct{}{}

	if state.Is("leader") {
		rf.becomeFollower(args.Term)
		rf.VoteFor = args.CandidateId
		reply.VoteGranted = true
	} else if state.Is("candidate") {
		// rf is old candidate (args.Term > currentTerm),
		// downgrade to candidate
		rf.becomeFollower(args.Term)
		// DPrintf("%s deny candidate request vote", rf)
		reply.VoteGranted = true
		return
	} else {
		rf.mu.Lock()
		rf.currentTerm = args.Term
		rf.VoteFor = args.CandidateId
		rf.mu.Unlock()
		reply.VoteGranted = true
	}

	DPrintf("%s vote for %d", rf, args.CandidateId)

	return
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int // index of log entry immediately preceding new ones
	PrevLogTerm  int // term of prevLogIndex entry
	Entries      []LogEntry
	LeaderCommit int // leader's commitIndex
}

type AppendEntriesReply struct {
	Term    int  // current term
	Success bool // append successed or rejected
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	state := rf.state
	currentTerm := rf.currentTerm
	rf.mu.Unlock()

	if !state.Is("follower") && currentTerm < args.Term {
		rf.becomeFollower(args.Term)
	}

	reply.Success = args.Term >= currentTerm
	reply.Term = currentTerm

	if !reply.Success {
		DPrintf("%s term mismatch, return false", rf)
		return
	}

	// reset heartbeat timer
	rf.resetTimerCh <- struct{}{}
	if len(args.Entries) == 0 {
		return
	}

	consistency := rf.isSameLogEntries(args.PrevLogIndex, args.PrevLogTerm)
	if !consistency {
		// append entries is not consistency, return false
		// then leader will decrease NextIndex and send AppendEntries RPC again
		DPrintf("%s logEntries consistency check failed: %+v <= %+v", rf, rf.logEntries, args.Entries)
		reply.Success = false
		return
	}

	rf.mu.Lock()
	DPrintf("%s append log %v", rf, args.Entries)
	rf.logEntries = append(rf.logEntries, args.Entries...)
	rf.mu.Unlock()

	for _, entry := range args.Entries {
		rf.applyEntry(entry)
	}

	return
}

func (rf *Raft) sendAppendEntriesRPC(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendHeartbeat() {
	rf.mu.Lock()
	term := rf.currentTerm
	peersLength := len(rf.peers)
	state := rf.state
	rf.mu.Unlock()

	if !state.Is("leader") {
		return
	}

	args := &AppendEntriesArgs{
		Term:    term,
		Entries: []LogEntry{},
	}

	for i := 0; i < peersLength; i++ {
		if i == rf.me {
			continue
		}
		go rf.sendAppendEntriesRPC(i, args, &AppendEntriesReply{})
	}
}

func (rf *Raft) sendAppendEntries(server int) *AppendEntriesReply {
	rf.mu.Lock()
	term := rf.currentTerm
	leaderCommitIndex := rf.CommitIndex
	rf.mu.Unlock()

	for {
		reply := AppendEntriesReply{
			Term:    -1,
			Success: false,
		}

		rf.mu.Lock()
		nextIndex := rf.NextIndex[server]
		DPrintf("nextIndex: %+v", nextIndex)
		var prevLog LogEntry
		if nextIndex == 0 {
			return &reply
		}
		if nextIndex-2 >= 0 {
			prevLog = rf.logEntries[nextIndex-2]
		}

		// entries that padding to send
		entries := rf.logEntries[nextIndex-1:]
		rf.mu.Unlock()

		DPrintf("AppendEntries to server{%d}: %+v, prev=%+v", server, entries, prevLog)

		// TODO handle NextIndex
		args := &AppendEntriesArgs{
			Term:         term,
			LeaderCommit: leaderCommitIndex,
			Entries:      entries,
			PrevLogTerm:  prevLog.Term,
			PrevLogIndex: prevLog.Index,
		}
		ok := rf.sendAppendEntriesRPC(server, args, &reply)
		if reply.Success {
			rf.mu.Lock()
			entry := getLastLogEntry(entries)
			rf.NextIndex[server] = entry.Index + 1
			rf.mu.Unlock()
			return &reply
		}

		if ok && !reply.Success && reply.Term > term {
			DPrintf("%s append failed, i = %d, term=%d", rf, rf.me, reply.Term)
			return &reply
		}
		if !ok {
			DPrintf("%s rpc failed, return ok=%+v", rf, ok)
			return &reply
		}

		rf.mu.Lock()
		rf.NextIndex[server] -= 1
		rf.mu.Unlock()
		if rf.NextIndex[server] < 1 {
			log.Fatalf("%s can not find log entries(%d): %+v", rf, server, entries)
		}
	}
}

func (rf *Raft) doAppendEntries(entry LogEntry) {
	rf.mu.Lock()
	peersLength := len(rf.peers)
	state := rf.state
	rf.mu.Unlock()

	if !state.Is("leader") {
		return
	}

	DPrintf("%s do append entry %v", rf, entry)
	var wg sync.WaitGroup

	results := make(chan AppendEntriesReply, peersLength)
	for i := 0; i < peersLength; i++ {
		if i == rf.me {
			continue
		}
		wg.Add(1)
		go func(i int) {
			result := rf.sendAppendEntries(i)
			results <- *result
			wg.Done()
		}(i)
	}
	wg.Wait()
	close(results)

	// all success
	if len(results) == 0 {
		return
	}
	maxTerm := 0
	for res := range results {
		DPrintf("results: %+v", res)
		if maxTerm < res.Term {
			maxTerm = res.Term
		}
	}
	DPrintf("found max term: %d", maxTerm)
	rf.mu.Lock()
	if rf.currentTerm < maxTerm {
		rf.currentTerm = maxTerm
		go rf.triggerElection()
	}
	rf.mu.Unlock()

	rf.applyEntry(entry)
}
