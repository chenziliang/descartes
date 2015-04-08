package main

import (
	"sync/atomic"
	"fmt"
	"strconv"
	"sort"
	"time"
	"math/rand"
)

var jobId int64 = 0

type Func func() error

type Job struct {
	f Func
	when int64
	interval int32
	id string
	props map[string]string
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

func New(f Func, when int64, interval int32, props map[string]string) *Job {
	return &Job {
		f: f,
		id: strconv.FormatInt(atomic.AddInt64(&jobId, 1), 10),
		when: when,
		interval: interval,
		props: props,
	}
}

func (job *Job) Less(rjob *Job) bool {
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

func (job *Job) Interval() int32 {
	return job.interval
}

func (job *Job) ExpirationTime() int64 {
	return job.when
}

func (job *Job) UpdateExpirationTime() int64 {
	job.when += int64(job.interval)
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

func Test() error {
	fmt.Println(time.Now().Unix())
	return nil
}

func main() {
	now := time.Now().Unix()
	fmt.Println(now)
	src := rand.NewSource(now)
	r := rand.New(src)

	var jobs JobList
	for i := 0; i < 1000000; i++ {
		job := New(Test, time.Now().Unix() + int64(r.Int() % 7), int32(r.Int() % 3), make(map[string]string))
	    jobs = append(jobs, job)
	}

	j := jobs[r.Int() % 1000000]
	fmt.Println(j)
	sort.Sort(jobs)
	fmt.Println(time.Now().Unix() - now)
	i := sort.Search(len(jobs), func(i int) bool { return !jobs[i].Less(j) })
	fmt.Println(time.Now().Unix() - now)
	fmt.Println(jobs[i])
}
