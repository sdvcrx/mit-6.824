package raft

import (
	"log"
	"math/rand"
)

// Debugging
const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

// Generate int with range in [from, to)
func RandomRange(from, to int) int {
	return rand.Intn(to-from) + from
}
