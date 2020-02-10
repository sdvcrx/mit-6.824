#!/bin/bash

echo "===> TestInitialElection2A"
go clean -testcache && go test -run TestInitialElection2A mitlab/raft
