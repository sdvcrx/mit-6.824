package raft

import (
	"strings"
)

type RaftState int

const (
	StateFollower = iota
	StateLeader
	StateCandidate
)

func (rs RaftState) String() string {
	switch rs {
	case StateLeader:
		return "Leader"
	case StateCandidate:
		return "Candidate"
	case StateFollower:
		return "Follower"
	}
	return "Follower"
}

func (rs RaftState) Is(role string) bool {
	rl := strings.ToLower(role)

	switch rl {
	case "leader":
		return rs == StateLeader
	case "candidate":
		return rs == StateCandidate
	case "follower":
		return rs == StateFollower
	}
	return rs == StateFollower
}

const VoteForNull = -1
