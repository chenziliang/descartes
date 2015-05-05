package base

import (
	"testing"
	"time"
)

func TestZooKeeperClient(t *testing.T) {
	config := BaseConfig{
		ZooKeeperServers: "172.16.107.153:2181",
	}

	allParticipants := []*ZooKeeperClient{}
	allGUIDs := []string{}

	for i := 0; i < 10; i++ {
		election := NewZooKeeperClient(config)
		if election == nil {
			t.Errorf("Failed to create ZooKeeperLeaderElection")
		}

		res, err := election.JoinElection("a")
		if err != nil {
			t.Errorf("Failed to JoinElection, error=%s", res)
		}
		allParticipants = append(allParticipants, election)
		allGUIDs = append(allGUIDs, res)
	}

	if res, err := allParticipants[0].IsLeader(allGUIDs[0]); err != nil || !res {
		t.Errorf("% should be leader", allGUIDs[0])
	}

	candidateChanges, err := allParticipants[8].WatchElectionParticipants()
	if err != nil {
		t.Errorf("Failed to WatchAllElectionParticipants, error=%s", err)
	}

	breakChan := make(chan bool, 1)
	done := make(chan bool, 1)
	go func() {
		for {
			select {
			case <-breakChan:
				done <- true
				return
			case <-candidateChanges:
				candidateChanges, err = allParticipants[8].WatchElectionParticipants()
			}
		}
	}()

	allParticipants[0].Close()
	if res, err := allParticipants[1].IsLeader(allGUIDs[1]); err != nil || !res {
		t.Errorf("%s should be leader", allGUIDs[1])
	}

	allParticipants[3].Close()
	time.Sleep(1 * time.Second)
	if res, err := allParticipants[1].IsLeader(allGUIDs[1]); err != nil || !res {
		t.Errorf("%s should be leader", allGUIDs[1])
	}

	allParticipants[1].Close()
	time.Sleep(1 * time.Second)
	if res, err := allParticipants[2].IsLeader(allGUIDs[2]); err != nil || !res {
		t.Errorf("%s should be leader", allGUIDs[2])
	}

	allParticipants[9].Close()
	time.Sleep(1 * time.Second)
	if res, err := allParticipants[2].IsLeader(allGUIDs[2]); err != nil || !res {
		t.Errorf("%s should be leader", allGUIDs[2])
	}

	err = allParticipants[8].CreateNode(HeartbeatRoot+"/"+"Kens-MacBook-Pro.local", nil, false, false)
	if err != nil {
		t.Errorf("Failed to crete node, error=%s", err)
	}

	err = allParticipants[8].DeleteNode(HeartbeatRoot + "/" + "Kens-MacBook-Pro.local")
	if err != nil {
		t.Errorf("Failed to delete node, error=%s", err)
	}

	time.Sleep(1 * time.Second)
	breakChan <- true
	<-done
}
