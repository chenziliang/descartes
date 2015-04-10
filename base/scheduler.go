package base

import (
	"sync"
	"sync/atomic"
	"time"
	"math/rand"
	"github.com/petar/GoLLRB/llrb"
	"github.com/golang/glog"
)


type Scheduler struct {
	jobs *llrb.LLRB
	wakeupChan chan int32
	doneChan chan bool
	started int32
	maxDelay int // nano second
	lockGuard sync.Mutex
}

const (
	WakeupNum int32 = 1
    TeardownNum int32 = 0
)

func NewScheduler() *Scheduler {
	return &Scheduler {
		jobs: llrb.New(),
		wakeupChan: make(chan int32, 100),
		doneChan: make(chan bool, 3),
	}
}

func (sched *Scheduler) Start() {
	if !atomic.CompareAndSwapInt32(&sched.started, 0, 1) {
	    glog.Info("Scheduler already started.")
		return
	}
	go sched.doJobs()
	glog.Info("Scheduler started.")
}

func (sched *Scheduler) Teardown() {
	if !atomic.CompareAndSwapInt32(&sched.started, 1, 0) {
		return
	}
	sched.wakeupChan <- TeardownNum
	<-sched.doneChan
	glog.Info("Scheduler exited.")
}

func (sched *Scheduler) AddJobs(jobs []* Job) {
	sched.lockGuard.Lock()
	defer sched.lockGuard.Unlock()

	now := time.Now().UnixNano()
	var r *rand.Rand
	var d int
	if sched.maxDelay > 0 {
	    r = rand.New(rand.NewSource(now))
	}

	for _, job := range jobs {
		if sched.maxDelay > 0 {
			d = r.Int() % sched.maxDelay
		}
		job.SetIntialExpirationTime(now + int64(d))
		sched.jobs.InsertNoReplace(job)
	}

	sched.wakeUp()
}

func (sched *Scheduler) UpdateJobs(jobs []*Job) {
	sched.lockGuard.Lock()
	defer sched.lockGuard.Unlock()

	for _, job := range(jobs) {
		sched.jobs.Delete(job)
		sched.jobs.InsertNoReplace(job)
	}
	sched.wakeUp()
}

func (sched *Scheduler) RemoveJobs(jobs []*Job) {
	sched.lockGuard.Lock()
	defer sched.lockGuard.Unlock()

	for _, job := range(jobs) {
		sched.jobs.Delete(job)
	}
	sched.wakeUp()
}

// SetMaxStartDelay: not immediately execute the job instead adding a random
// sleep before executing it
// @d: in nanosecond
func (sched *Scheduler) SetMaxStartDelay(d int) {
	sched.maxDelay = d
}

func (sched *Scheduler) wakeUp() {
	sched.wakeupChan <- WakeupNum
}

func (sched *Scheduler) doJobs() {
	L:
	for {
		sleep_time, jobs := sched.getReadyJobs()
		sched.executeJobs(jobs)
		select {
		case <-time.After(sleep_time):
			continue
		case v := <-sched.wakeupChan:
			if v == TeardownNum {
				break L
			}
		}
	}
	sched.doneChan <- true
	glog.Info("Scheduler is going to exit.")
}

func (sched *Scheduler) getReadyJobs() (sleep_time time.Duration, jobs []*Job) {
	sleep_time = time.Second / 10
	var readyJobs []*Job
	var job *Job

	sched.lockGuard.Lock()
	defer sched.lockGuard.Unlock()

	now := time.Now().UnixNano()
	for sched.jobs.Len() > 0 {
		job = sched.jobs.Min().(*Job)
		if job.ExpirationTime() < now {
			readyJobs = append(readyJobs, job)
			sched.jobs.DeleteMin()
		} else {
			break
		}
	}

	for _, job := range readyJobs {
		if job.Interval() > 0 {
			job.UpdateExpirationTime()
            sched.jobs.InsertNoReplace(job)
		}
	}

	if sched.jobs.Len() > 0 {
		job = sched.jobs.Min().(*Job)
		sleep_time = time.Duration(job.ExpirationTime() - now)
	}

	return sleep_time, readyJobs
}

func (sched *Scheduler) executeJobs(jobs []*Job) {
	for _, job := range(jobs) {
		job.f()
	}
}
