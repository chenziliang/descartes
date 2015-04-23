package base

import (
	"github.com/golang/glog"
	"github.com/petar/GoLLRB/llrb"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

type Scheduler struct {
	jobs       *llrb.LLRB
	wakeupChan chan int32
	doneChan   chan bool
	started    int32
	maxDelay   int // nano second
	lockGuard  sync.Mutex
}

const (
	wakeupNum   int32 = 1
	teardownNum int32 = 0
)

func NewScheduler() *Scheduler {
	return &Scheduler{
		jobs:       llrb.New(),
		wakeupChan: make(chan int32, 100),
		doneChan:   make(chan bool, 3),
	}
}

func (sched *Scheduler) Start() {
	if !atomic.CompareAndSwapInt32(&sched.started, 0, 1) {
		glog.Infof("Scheduler already started.")
		return
	}
	go sched.doJobs()
	glog.Infof("Scheduler started...")
}

func (sched *Scheduler) Stop() {
	if !atomic.CompareAndSwapInt32(&sched.started, 1, 0) {
		glog.Infof("Scheduler already stopped.")
		return
	}
	sched.wakeupChan <- teardownNum
	<-sched.doneChan
	pivot := NewJob(JobFunc(nil), -1, 0, nil)
	sched.jobs.AscendGreaterOrEqual(pivot, func(i llrb.Item) bool {
		i.(Job).Stop()
		return true
	})
	glog.Infof("Scheduler stopped...")
}

func (sched *Scheduler) AddJobs(jobs []Job) {
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
		job.Start()
	}

	sched.wakeUp()
}

func (sched *Scheduler) UpdateJobs(jobs []Job) {
	sched.lockGuard.Lock()
	defer sched.lockGuard.Unlock()

	for _, job := range jobs {
		sched.jobs.Delete(job)
		sched.jobs.InsertNoReplace(job)
	}
	sched.wakeUp()
}

func (sched *Scheduler) RemoveJobs(jobs []Job) {
	sched.lockGuard.Lock()
	defer sched.lockGuard.Unlock()

	for _, job := range jobs {
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
	sched.wakeupChan <- wakeupNum
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
			if v == teardownNum {
				break L
			}
		}
	}
	sched.doneChan <- true
	glog.Infof("Scheduler is going to exit.")
}

func (sched *Scheduler) getReadyJobs() (sleep_time time.Duration, jobs []Job) {
	sleep_time = time.Second / 10
	var readyJobs []Job
	var job Job

	sched.lockGuard.Lock()
	defer sched.lockGuard.Unlock()

	now := time.Now().UnixNano()
	for sched.jobs.Len() > 0 {
		job = sched.jobs.Min().(Job)
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
		job = sched.jobs.Min().(Job)
		sleep_time = time.Duration(job.ExpirationTime() - now)
	}

	return sleep_time, readyJobs
}

func (sched *Scheduler) executeJobs(jobs []Job) {
	for _, job := range jobs {
		job.Callback()
	}
}
