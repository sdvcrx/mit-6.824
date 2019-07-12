package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.

	taskQueue := make(chan int, ntasks)
	done := make(chan bool)    // use it to close all goroutines
	var wg sync.WaitGroup

	for i := 0; i < ntasks; i++ {
		taskQueue <- i
		wg.Add(1)
	}

	go func() {
		for {
			select {
			case <-done:
				debug("Worker quit...\n")
				return
			case worker := <-registerChan:
				go func(wk string) {
					for {
						select {
						case <-done:
							debug("Worker quit...\n")
							return
						case task := <-taskQueue:
							debug("start to DoTask, task=%d, file: %s\n", task, mapFiles[task])
							args := DoTaskArgs{
								JobName: jobName,
								File: mapFiles[task],
								TaskNumber: task,
								Phase: phase,
								NumOtherPhase: n_other,
							}
							ok := call(wk, "Worker.DoTask", args, new(struct {}))
							if !ok {
								taskQueue <- task    // push failed tash back to taskQueue
								fmt.Printf("Worker DoTask error: worker=%s, task=%d\n", wk, task)
							} else {
								wg.Done()
							}
						}
					}
				}(worker)
			}
		}
	}()

	// block until all tasks finished
	wg.Wait()

	fmt.Printf("Schedule: %v done\n", phase)
}
