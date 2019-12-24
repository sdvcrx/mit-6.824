package raft

type RaftState int

const (
	StateFollower = iota
	StateLeader
	StateCandidate
)

const VoteForNull = -1
