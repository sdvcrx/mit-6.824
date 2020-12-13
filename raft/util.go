package raft

import (
	"fmt"
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = 1

func init() {
	rand.Seed(time.Now().UnixNano())
	log.SetFlags(log.Lmicroseconds)
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func TPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		fmt.Printf(format, a...)
	}
	return
}

// Generate int with range in [from, to)
func RandomRange(from, to int) int {
	return rand.Intn(to-from) + from
}

type genDurationFunc func() time.Duration

type RaftTimer struct {
	timer    *time.Timer
	duration time.Duration
	genFunc  genDurationFunc
	C        <-chan time.Time
}

func (rt *RaftTimer) Reset() {
	rt.duration = rt.genFunc()

	// stop before reset timer
	// https://pkg.go.dev/time?tab=doc#Timer.Reset
	rt.timer.Stop()
	rt.timer.Reset(rt.duration)
}

func (rt *RaftTimer) Stop() {
	rt.timer.Stop()
}

func (rt *RaftTimer) String() string {
	return fmt.Sprintf("RaftTimer(duration=%s)", rt.duration)
}

func pipe(in <-chan time.Time) <-chan time.Time {
	out := make(chan time.Time, 1)

	go func() {
		for ele := range in {
			out <- ele
		}
		close(out)
	}()

	return out
}

// election timeout, random in [150, 300)
func NewElectionTimer() *RaftTimer {
	genFunc := func() time.Duration {
		return time.Duration(RandomRange(150, 300)) * time.Millisecond
	}

	dur := genFunc()
	timer := time.NewTimer(dur)

	return &RaftTimer{
		timer:    timer,
		C:        pipe(timer.C),
		duration: dur,
		genFunc:  genFunc,
	}
}

// leader heartbeat timer
func NewHeartbeatTimer() *RaftTimer {
	dur := 100 * time.Millisecond

	genFunc := func() time.Duration {
		return dur
	}
	timer := time.NewTimer(dur)

	return &RaftTimer{
		duration: dur,
		C:        pipe(timer.C),
		timer:    timer,
		genFunc:  genFunc,
	}
}

func genNextIndex(val int, len int) []int {
	nextIndex := make([]int, len)

	for i := 0; i < len; i++ {
		nextIndex[i] = val
	}

	return nextIndex
}

func getLastLogEntry(entries []LogEntry) *LogEntry {
	entriesLength := len(entries)
	if entriesLength == 0 {
		return &LogEntry{}
	}

	return &entries[entriesLength-1]
}

func getLastLogIndex(entries []LogEntry) int {
	entry := getLastLogEntry(entries)
	return entry.Index
}
