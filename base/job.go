package base

import (
	"sync/atomic"
	"fmt"
	"strconv"
	"time"
	"math/rand"
	"github.com/petar/GoLLRB/llrb"
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


func buildLLRBTree(jobs []*Job) *llrb.LLRB {
	now := time.Now().Unix()

	tree := llrb.New()
	for _, job := range jobs {
	    tree.InsertNoReplace(job)
	}
	fmt.Printf("Build llrb tree: %d sec\n", time.Now().Unix() - now)
	return tree
}

func buildJobs(n int) []*Job {
	now := time.Now().Unix()

	src := rand.NewSource(now)
	r := rand.New(src)

	jobs := make([]*Job, 0, n)
	for i := 0; i < n; i++ {
		job := New(Test, time.Now().Unix() + int64(r.Int() % 7), int32(r.Int() % 3), make(map[string]string))
		jobs = append(jobs, job)
	}
	fmt.Printf("Build %d jobs tree: %d sec\n", n, time.Now().Unix() - now)
	return jobs
}

func _main() {
	n := 1000000
	index := n / 2
	jobs := buildJobs(n)
	tree := buildLLRBTree(jobs)
	if tree.Len() != n {
		panic("Something is wrong, the number is not right")
	}

	now := time.Now().Unix()
	lastMin := tree.DeleteMin()
	for tree.Len() != 0 {
		nowMin := tree.DeleteMin()
		if !lastMin.Less(nowMin) {
			panic("Something is wrong with the llrb")
		}
	}
	fmt.Printf("Delete all mins %d sec\n", time.Now().Unix() - now)

	tree = buildLLRBTree(jobs)
	now = time.Now().Unix()
	lastMax := tree.DeleteMax()
	for tree.Len() != 0 {
		nowMax := tree.DeleteMax()
		if lastMax.Less(nowMax) {
			panic("Something is wrong with the llrb")
		}
	}
	fmt.Printf("Delete all maxs %d sec\n", time.Now().Unix() - now)

	tree = buildLLRBTree(jobs)
	now = time.Now().UnixNano()
	_ = tree.Max()
	fmt.Printf("Look for max: %d nano sec\n", time.Now().UnixNano() - now)

	now = time.Now().UnixNano()
	_ = tree.Min()
	fmt.Printf("Look for min: %d nano sec\n", time.Now().UnixNano() - now)

	for i := 0; i < 10; i++ {
		now = time.Now().UnixNano()
		item := tree.Get(jobs[index + i])
		fmt.Printf("Look for rand: %d nano sec\n", time.Now().UnixNano() - now)
		if item.(*Job).Id() != jobs[index + i].Id() {
			panic("Something wrong has happended")
		}
	}
}
