#!/bin/bash

cases=("TestInitialElection2A" "TestReElection2A")

for testcase in "${cases[@]}"; do
    echo "===> ${testcase}"
    go test -run "$testcase" mitlab/raft
done
