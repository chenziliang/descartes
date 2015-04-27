package base

import (
	"fmt"
	"github.com/petar/GoLLRB/llrb"
	"strconv"
	"sync/atomic"
)

type JobParam interface {}

type JobFunc func(params JobParam) error

type Job interface {
	ResetFunc(f JobFunc)
	Less(other llrb.Item) bool
	String() string
	Id() string
	Interval() int64
	ExpirationTime() int64
	UpdateExpirationTime() int64
	SetIntialExpirationTime(when int64)
	Start()
	Stop()
	Callback()
}

var jobId int64 = 0

type BaseJob struct {
	f        JobFunc
	when     int64 // absolut nano seconds since epoch
	interval int64 // nano seconds
	id       string
	params   JobParam
}

type JobList []Job

// JobList implements sort.Interface
func (jobs JobList) Len() int {
	return len(jobs)
}

func (jobs JobList) Swap(i, j int) {
	jobs[i], jobs[j] = jobs[j], jobs[i]
}

func (jobs JobList) Less(i, j int) bool {
	return jobs[i].Less(jobs[j])
}

func NewJob(f JobFunc, when int64, interval int64, params JobParam) *BaseJob {
	return &BaseJob{
		f:        f,
		id:       strconv.FormatInt(atomic.AddInt64(&jobId, 1), 10),
		when:     when,
		interval: interval,
		params:    params,
	}
}

func (job *BaseJob) ResetFunc(f JobFunc) {
	job.f = f
}

func (job *BaseJob) Less(other llrb.Item) bool {
	rjob := other.(Job)
	if job.ExpirationTime() < rjob.ExpirationTime() {
		return true
	}

	if job.ExpirationTime() == rjob.ExpirationTime() {
		return job.Id() < rjob.Id()
	}
	return false
}

func (job *BaseJob) String() string {
	return fmt.Sprintf("%d(%s)", job.when, job.id)
}

func (job *BaseJob) Id() string {
	return job.id
}

func (job *BaseJob) Interval() int64 {
	return job.interval
}

func (job *BaseJob) ExpirationTime() int64 {
	return job.when
}

func (job *BaseJob) UpdateExpirationTime() int64 {
	job.when += job.interval
	return job.when
}

func (job *BaseJob) SetIntialExpirationTime(when int64) {
	if job.when == 0 {
		job.when = when
	}
}

func (job *BaseJob) Start() {
}

func (job *BaseJob) Stop() {
}

func (job *BaseJob) Callback() {
	job.f(job.params)
}
