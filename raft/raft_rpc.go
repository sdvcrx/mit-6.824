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
	lastLog := getLastLogEntry(rf.logEntries)
	rf.mu.Unlock()

	reply.Term = currentTerm

	// Your code here (2A, 2B).
	if args.Term <= currentTerm {
		rf.mu.Lock()
		rf.VoteFor = VoteForNull
		rf.mu.Unlock()
		DPrintf("%s receive invalid rpc.Term: %d, from: %d", rf, args.Term, args.CandidateId)
		reply.VoteGranted = false
		return
	}
	/**
	 * NOTE
	 * candidate’s log is at least as up-to-date as receiver’s log then vote
	 * http://vearne.cc/archives/1510
	 * candidate.LastLogTerm >= receiver.LastLogTerm
	 * candidate.LastLogIndex >= receiver.LastLogIndex
	 */
	if args.LastLogIndex < lastLog.Index || args.LastLogTerm < lastLog.Term {
		DPrintf("%s: candidate(%d) log is not up-to-date, rejected", rf, args.CandidateId)
		reply.VoteGranted = false
		rf.VoteFor = VoteForNull
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

	// prevent old leader rejoin
	if !state.Is("follower") && currentTerm < args.Term {
		rf.becomeFollower(args.Term)
	}

	// update Follower currentTerm
	// FIXME Reply false if term < currentTerm (§5.1)
	if currentTerm < args.Term {
		rf.mu.Lock()
		rf.currentTerm = args.Term
		rf.mu.Unlock()
	}

	reply.Success = args.Term >= currentTerm
	reply.Term = currentTerm

	if !reply.Success {
		DPrintf("%s term mismatch, got: %d, current: %d, return false", rf, args.Term, currentTerm)
		return
	}

	// DPrintf("%s AppendEntries %+v ==== %+v", rf, args, rf.logEntries)
	// reset heartbeat timer
	rf.resetTimerCh <- struct{}{}

	if len(args.Entries) == 0 {
		// handle heartbeat msg
		// 将 rf.logEntries 中未 apply 的消息 apply to state machine
		if rf.LastApplied < args.LeaderCommit {
			DPrintf("heartbeat %+v LastApplied=%d, CommitIndex=%d %+v", args, rf.CommitIndex, rf.LastApplied, rf.logEntries)

			rf.applyEntries()
		}
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
	DPrintf("%s append log %+v", rf, args.Entries)
	if len(rf.logEntries) > args.LeaderCommit {
		// commit logEntries (may strip uncommitted logs)
		rf.logEntries = append(rf.logEntries[:args.LeaderCommit], args.Entries...)
	} else {
		rf.logEntries = append(rf.logEntries, args.Entries...)
	}
	last := getLastLogEntry(rf.logEntries)
	rf.CommitIndex = last.Index
	DPrintf("%s set commitIndex to %d", rf, rf.CommitIndex)
	rf.mu.Unlock()

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
	commitIndex := rf.CommitIndex
	rf.mu.Unlock()

	if !state.Is("leader") {
		return
	}

	args := &AppendEntriesArgs{
		Term:         term,
		Entries:      []LogEntry{},
		LeaderCommit: commitIndex,
	}

	for i := 0; i < peersLength; i++ {
		if i == rf.me {
			continue
		}
		go rf.sendAppendEntriesRPC(i, args, &AppendEntriesReply{})
	}
}

// Send AppendEntries RPC request and make sure it return success (Leader only)
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
		DPrintf("leader.SendAppendEntries nextIndex: %+v", nextIndex)
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
		DPrintf("server{%d} relay: %+v", server, reply)
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

		DPrintf("%s decrease NextIndex", rf)
		rf.mu.Lock()
		rf.NextIndex[server] -= 1
		rf.mu.Unlock()
		if rf.NextIndex[server] < 1 {
			log.Fatalf("%s can not find log entries(%d): %+v", rf, server, entries)
		}
	}
}

// leader
func (rf *Raft) doAppendEntries(entry LogEntry) {
	rf.mu.Lock()
	peersLength := len(rf.peers)
	state := rf.state
	rf.mu.Unlock()

	if !state.Is("leader") {
		return
	}

	DPrintf("%s do append entry %+v", rf, entry)
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
	successCount := 1
	for res := range results {
		DPrintf("results: %+v", res)
		if maxTerm < res.Term {
			maxTerm = res.Term
		}
		if res.Success {
			successCount += 1
		}
	}
	DPrintf("found max term: %d", maxTerm)
	rf.mu.Lock()
	if rf.currentTerm < maxTerm {
		rf.currentTerm = maxTerm
		go rf.triggerElection()
	}
	rf.mu.Unlock()

	if successCount < peersLength/2+1 {
		DPrintf("doAppendEntries failed to reach a majority of servers, got: %d, expect: %d", successCount, peersLength/2+1)
		return
	}
	rf.applyEntry(entry)
}
