package base

import (
	"fmt"
	"github.com/golang/glog"
	"github.com/samuel/go-zookeeper/zk"
	"strings"
	"strconv"
	"time"
)


type ZooKeeperLeaderElection struct {
	conn *zk.Conn
	config BaseConfig
}


const (
	electionPath = "/descartes_election"
)


func NewZooKeeperLeaderElection(serverConfig BaseConfig) *ZooKeeperLeaderElection {
	if servers, ok := serverConfig[ZooKeeperServers]; !ok || servers == "" {
		glog.Errorf("Missing ZooKeeper server configuration")
		return nil
	}

	servers := strings.Split(serverConfig[ZooKeeperServers], ";")

	conn, _, err := zk.Connect(servers, 30 * time.Second)
	if err != nil {
		glog.Errorf("Failed to create ZooKeeper Connection, error=%s", err)
		return nil
	}

	if _, ok := serverConfig[ZooKeeperElectionPath]; !ok {
		glog.Infof("Election path not found, use default=%s", electionPath)
		serverConfig[ZooKeeperElectionPath] = electionPath
	}

	path := serverConfig[ZooKeeperElectionPath]

	ok, _, err := conn.Exists(path)
	if err != nil {
		glog.Errorf("Failed to check election path, error=%s", err)
		return nil
	}

	if !ok {
		glog.Infof("Election path=%s doesn't exist, create", path)
		_, err = conn.Create(path, nil, 0, zk.WorldACL(zk.PermAll))
		if err != nil && err != zk.ErrNodeExists {
			glog.Errorf("Failed to create election path=%s, error=%s", path, err)
			return nil
		}
	}

	return &ZooKeeperLeaderElection{
		conn: conn,
		config: serverConfig,
	}
}

func (election *ZooKeeperLeaderElection) JoinElection(path string) (string, error) {
	fullPath := fmt.Sprintf("%s/%s_", election.config[ZooKeeperElectionPath], path)
	res, err := election.conn.CreateProtectedEphemeralSequential(fullPath, nil, zk.WorldACL(zk.PermAll))
	if err != nil {
		glog.Errorf("Failed to join election path=%s, error=%s", path, err)
		return "", err
	}
	glog.Infof("%s joined the leader election.", path)
	return res, nil
}

func (election *ZooKeeperLeaderElection) AllCandidates() ([]string, error) {
	children, _, err := election.conn.Children(election.config[ZooKeeperElectionPath])
	if err != nil {
		return nil, err
	}
	return children, nil
}

// if get a event from event chan, something has happened
// call IsLeader to see if itself is a leader
func (election *ZooKeeperLeaderElection) MonitorCandidateChanges() (<-chan zk.Event, error) {
	_, _, eventChan, err := election.conn.ChildrenW(election.config[ZooKeeperElectionPath])
	return eventChan, err
}

// guidPath is the result returned in JoinElection
// Compare all children in the election path, select the one which have min
// sequential number
func (election *ZooKeeperLeaderElection) IsLeader(guidPath string) (bool, error) {
	candidates, err := election.AllCandidates()
	if err != nil {
		return false, err
	}

	candidateNum, err := strconv.ParseInt(guidPath[len(guidPath) - 10:], 10, 32)
	if err != nil {
		glog.Errorf("Failed to parse %s to int", guidPath[len(guidPath) - 10:])
		return false, err
	}

	var otherCandidates []int32
	for _, candidate := range candidates {
	    num, err := strconv.ParseInt(candidate[len(candidate) - 10:], 10, 32)
		if err != nil {
			glog.Errorf("Failed to parse %s to int", candidate[len(candidate) - 10:])
			return false, err
		}
		otherCandidates = append(otherCandidates, int32(num))
	}

	// FIXME overfollow and rejoin
	for _, num := range otherCandidates {
		if num < int32(candidateNum) {
			return false, nil
		}
	}
	return true, nil
}

func (election *ZooKeeperLeaderElection) Close() {
	election.conn.Close()
}
