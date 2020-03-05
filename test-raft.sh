#!/bin/bash

cases=(
    "TestInitialElection2A"
    "TestReElection2A"
    "TestBasicAgree2B"
)

for testcase in "${cases[@]}"; do
    echo "===> ${testcase}"
    go test -run "$testcase" mitlab/raft
    go test -race -run "$testcase" mitlab/raft
done
