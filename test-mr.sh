#!/bin/bash
echo ""
echo "==> Part I"
go test -run Sequential mitlab/mapreduce/...
echo ""
echo "==> Part II"
pushd ./cmd/wc/
./test-wc.sh > /dev/null
popd
echo ""
echo "==> Part III"
go test -run TestParallel mitlab/mapreduce/...
echo ""
echo "==> Part IV"
go test -run Failure mitlab/mapreduce/...
echo ""
echo "==> Part V (inverted index)"
pushd ./cmd/ii/
./test-ii.sh > /dev/null
popd
