package base


import (
	"testing"
	"time"
	"fmt"
)


func testLLRB() {
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

func TestLLRB(t *testing.T) {
	testLLRB()
}
