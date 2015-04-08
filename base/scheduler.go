package base

import (
	"sync/atomic"
	"github.com/petar/GoLLRB/llrb"
)


type Scheduler struct {
	jobs *llrb.LLRB
	wakupChan chan int32
	doneChan chan bool
	started int32
}

const (
	WakeupNum int32 = 1
    TeardownNum int32 = 0
)

func New() *Scheduler {
	return &Scheduler {
		jobs: llrb.New(),
		wakeupChan: make(chan int32, 100),
		doneChan: make(chan bool, 3),
	}
}

func (sched *Scheduler) Start() {
	if !atomic.CompareAndSwap(&sched.started, 0, 1) {
		return
	}
	go sched.doJobs()
}

func (sched *Scheduler) Teardown() {
	if !atomic.CompareAndSwap(&sched.started, 1, 0) {
		return
	}
	wakeupChan <- TeardownNum
	<-doneChan
}


func (sched *Scheduler) doJobs() {
	L:
	for {
    	sleep_time, jobs := sched.getReadyJobs()
    	sched.executeJobs(jobs)
    	select {
    	case <-time.After(sleep_time):
    		continue
    	case v := <-wakeupChan:
    		if v == TeardownNum {
				break L
    		}
    	}
	}
}


func (sched *Scheduler) getReadyJobs() (sleep_time int64, jobs) {
}
