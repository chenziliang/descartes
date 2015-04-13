package base

import (
	"fmt"
	"github.com/petar/GoLLRB/llrb"
	"strconv"
	"sync/atomic"
)

var jobId int64 = 0

type Func func() error

type Job struct {
	f        Func
	when     int64 // absolut nano seconds since epoch
	interval int64 // nano seconds
	id       string
	props    map[string]string
}

type JobList []*Job

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

func NewJob(f Func, when int64, interval int64, props map[string]string) *Job {
	return &Job{
		f:        f,
		id:       strconv.FormatInt(atomic.AddInt64(&jobId, 1), 10),
		when:     when,
		interval: interval,
		props:    props,
	}
}

func (job *Job) Less(other llrb.Item) bool {
	rjob := other.(*Job)
	if job.when < rjob.when {
		return true
	}

	if job.when == rjob.when {
		return job.id < rjob.id
	}
	return false
}

func (job *Job) String() string {
	return fmt.Sprintf("%d(%s)", job.when, job.id)
}

func (job *Job) Id() string {
	return job.id
}

func (job *Job) Interval() int64 {
	return job.interval
}

func (job *Job) ExpirationTime() int64 {
	return job.when
}

func (job *Job) UpdateExpirationTime() int64 {
	job.when += job.interval
	return job.when
}

func (job *Job) SetIntialExpirationTime(when int64) {
	if job.when == 0 {
		job.when = when
	}
}

func (job *Job) Prop(key string) string {
	return job.props[key]
}

func (job *Job) SetProp(key, value string) {
	job.props[key] = value
}
