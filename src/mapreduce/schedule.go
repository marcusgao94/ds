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
	//
	// Your code here (Part III, Part IV).
	//

	// use a map maintain a task success or not
	type MT struct {
		sync.Mutex
		success map[int]bool // true is being processed, no key is success
	}
	mt := new(MT)
	mt.success = make(map[int]bool)
	for i := 0; i < ntasks; i++ {
		mt.success[i] = false
	}
	var done sync.WaitGroup
	for {
		// if all tasks success, map should be empty
		mt.Lock()
		if len(mt.success) == 0 {
			mt.Unlock()
			break
		}
		// get a unsuccess task from map
		i := -1
		for k, _ := range mt.success {
			if !mt.success[k] {
				i = k
				break
			}
		}
		if i == -1 {
			mt.Unlock()
			continue
		}
		mt.success[i] = true
		mt.Unlock()
		w := <-registerChan
		done.Add(1)
		// start a thread to call worker
		go func(worker string, idx int) {
			defer done.Done()
			args := DoTaskArgs{jobName, mapFiles[idx], phase, idx, n_other}
			succ := call(worker, "Worker.DoTask", &args, &args)
			if succ {
				mt.Lock()
				delete(mt.success, idx)
				mt.Unlock()
				select {
				case registerChan <- worker:
				default:
				}
			} else {
				mt.Lock()
				mt.success[idx] = false
				mt.Unlock()
			}
		}(w, i)
	}
	done.Wait()

	fmt.Printf("Schedule: %v done\n", phase)
}
