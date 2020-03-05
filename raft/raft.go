package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"fmt"
	"log"
	"sync"

	"mitlab/labrpc"
	"time"
)

// import "bytes"
// import "labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// Log entry
type LogEntry struct {
	Index   int // log index. identifying its position in the log
	Term    int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	state  RaftState // server state
	Killed bool      // server is killed

	// Persistent state on all servers:
	// (Updated on stable storage before responding to RPCs)
	currentTerm int // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	VoteFor     int // candidateId that received vote in current term (or null if none)

	logEntries []LogEntry // log entries

	// Volatile state on all servers:
	CommitIndex int // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	LastApplied int // index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	// Volatile state on leaders:
	// (Reinitialized after election)
	NextIndex  []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	MatchIndex []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	electionTimer  *RaftTimer
	heartbeatTimer *RaftTimer
	resetTimerCh   chan struct{}
	killCh         chan struct{}
	applyCh        chan ApplyMsg
}

func (rf *Raft) String() string {
	return fmt.Sprintf("Raft(index=%d|state=%s|currentTerm=%d)", rf.me, rf.state, rf.currentTerm)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = (rf.state == StateLeader)

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()

	index := -1
	term := rf.currentTerm
	isLeader := rf.state.Is("leader")

	if !isLeader {
		rf.mu.Unlock()
		return index, term, isLeader
	}

	entryIndex := rf.LastApplied + 1
	entry := LogEntry{
		Index:   entryIndex,
		Term:    term,
		Command: command,
	}
	DPrintf("%s append entry: %v", rf, command)
	rf.logEntries = append(rf.logEntries, entry)
	rf.LastApplied = entryIndex

	rf.mu.Unlock()

	rf.doAppendEntries(entry)
	DPrintf("finished %s", rf)
	msg := ApplyMsg{
		CommandValid: true,
		Command:      command,
		CommandIndex: entryIndex,
	}
	rf.applyCh <- msg
	DPrintf("<- %s, command %+v", rf, msg)

	// Your code here (2B).

	return entryIndex, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	// if rf.electionTimer != nil {
	// 	rf.electionTimer.Stop()
	// }
	// if rf.heartbeatTimer != nil {
	// 	rf.heartbeatTimer.Stop()
	// }
	// rf.killCh <- struct{}{}
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	// rf.Killed = true
	// DPrintf("%s killed", rf)
}

// check RPC's term > currentTerm
func (rf *Raft) isValidRPC(term int) bool {
	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower (§5.1)
	return term <= rf.currentTerm
}

func (rf *Raft) checkElectionTimeout() {
	for {
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()

		if state.Is("leader") {
			rf.electionTimer.Stop()
			return
		}

		select {
		case <-rf.electionTimer.C:
			rf.triggerElection()
		case <-rf.resetTimerCh:
			rf.electionTimer.Reset()
		case <-rf.killCh:
			return
		}
	}
}

func (rf *Raft) heartbeat() {
	for {
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()

		if !state.Is("leader") {
			rf.heartbeatTimer.Stop()
			return
		}

		select {
		case <-rf.heartbeatTimer.C:
			rf.heartbeatTimer.Reset()
			rf.sendHeartbeat()
		case <-rf.killCh:
			return
		}
	}
}

func (rf *Raft) becomeLeader() {
	rf.mu.Lock()
	if rf.state.Is("leader") {
		log.Fatalf("%s is leader already", rf)
	}

	rf.state = StateLeader
	DPrintf("%s become leader", rf)
	rf.mu.Unlock()

	rf.electionTimer.Stop()

	rf.heartbeatTimer = NewHeartbeatTimer()
	go rf.heartbeat()
	rf.sendHeartbeat()
}

func (rf *Raft) becomeCandidate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.state = StateCandidate
	rf.currentTerm++
	rf.VoteFor = rf.me
}

func (rf *Raft) becomeFollower(term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state.Is("follower") {
		log.Fatalf("%s is follower already", rf)
	}

	rf.state = StateFollower
	rf.currentTerm = term
	rf.VoteFor = VoteForNull
	rf.electionTimer.Reset()
	go rf.checkElectionTimeout()
	// DPrintf("%s ====> become a follower", rf)
}

func (rf *Raft) triggerElection() {
	rf.becomeCandidate()
	rf.mu.Lock()
	DPrintf("%s timeout, start election", rf)
	term := rf.currentTerm
	peersLength := len(rf.peers)
	rf.mu.Unlock()

	var wg sync.WaitGroup
	result := make(chan RequestVoteReply, peersLength-1)

	for i := 0; i < peersLength; i++ {
		if i == rf.me {
			continue
		}
		wg.Add(1)
		go func(i int) {
			req := RequestVoteArgs{
				Term:        term,
				CandidateId: rf.me,
			}
			reply := RequestVoteReply{}
			rf.sendRequestVote(i, &req, &reply)
			result <- reply
			wg.Done()
		}(i)
	}

	done := make(chan struct{}, 1)
	go func() {
		wg.Wait()
		done <- struct{}{}
	}()

	select {
	case <-time.After(100 * time.Millisecond):
		DPrintf("raft election timeout")
	case <-done:
		close(done)
		close(result)
	}

	vote := 1
	resultNum := len(result)
	for i := 0; i < resultNum; i++ {
		item := <-result
		if item.VoteGranted {
			vote++
		}
	}

	if vote >= peersLength/2+1 {
		DPrintf("%s Vote granted, got %d votes", rf, vote)
		rf.becomeLeader()
	} else {
		DPrintf("%s Vote deny, got %d votes", rf, vote)
		rf.electionTimer.Reset()
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = StateFollower
	rf.VoteFor = VoteForNull

	rf.resetTimerCh = make(chan struct{}, 1)
	rf.killCh = make(chan struct{}, 1)
	rf.electionTimer = NewElectionTimer()
	rf.applyCh = applyCh

	// TODO create a background goroutine that will kick off leader election periodically by
	//        sending out RequestVote RPCs when it hasn't heard from another peer for a while.
	// go rf.heartbeat()
	go rf.checkElectionTimeout()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
