package base

import (
	"testing"
	"time"
)


func TestLeaderElection(t *testing.T) {
	config := BaseConfig{
		ZooKeeperServers: "172.16.107.153:2181",
	}

	allCandidates := []*ZooKeeperLeaderElection{}
	allGUIDs := []string{}

	for i := 0; i < 10; i++ {
		election := NewZooKeeperLeaderElection(config)
		if election == nil {
			t.Errorf("Failed to create ZooKeeperLeaderElection")
		}

		res, err := election.JoinElection("a")
		if err != nil {
			t.Errorf("Failed to JoinElection, error=%s", res)
		}
		allCandidates = append(allCandidates, election)
		allGUIDs = append(allGUIDs, res)
	}

	if res, err := allCandidates[0].IsLeader(allGUIDs[0]); err != nil || ! res {
		t.Errorf("% should be leader", allGUIDs[0])
	}

	candidateChanges, err := allCandidates[8].MonitorCandidateChanges()
	if err != nil {
		t.Errorf("Failed to MonitorCandidateChanges, error=%s", err)
	}

	done := make(chan bool, 1)
	go func() {
		for{
			select{
			case <-done:
				done <- true
				return
			case <-candidateChanges:
				;
			}
		}
	}()

	allCandidates[0].Close()
	if res, err := allCandidates[1].IsLeader(allGUIDs[1]); err != nil || ! res {
		t.Errorf("% should be leader", allGUIDs[1])
	}

	allCandidates[3].Close()
	if res, err := allCandidates[1].IsLeader(allGUIDs[1]); err != nil || ! res {
		t.Errorf("% should be leader", allGUIDs[1])
	}

	allCandidates[1].Close()
	if res, err := allCandidates[2].IsLeader(allGUIDs[2]); err != nil || ! res {
		t.Errorf("% should be leader", allGUIDs[2])
	}

	allCandidates[9].Close()
	if res, err := allCandidates[2].IsLeader(allGUIDs[2]); err != nil || ! res {
		t.Errorf("% should be leader", allGUIDs[2])
	}

	time.Sleep(1 * time.Second)
	done <- true
	<-done
}
