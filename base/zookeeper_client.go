package base

import (
	"fmt"
	"github.com/golang/glog"
	"github.com/samuel/go-zookeeper/zk"
	"path"
	"strconv"
	"strings"
	"time"
)

const (
	Root          = "/descartes"
	ElectionRoot  = Root + "/election"
	HeartbeatRoot = Root + "/heartbeat"
)

type ZooKeeperClient struct {
	conn   *zk.Conn
	config BaseConfig
}

func NewZooKeeperClient(serverConfig BaseConfig) *ZooKeeperClient {
	if servers, ok := serverConfig[ZooKeeperServers]; !ok || servers == "" {
		glog.Errorf("Missing ZooKeeper server configuration")
		return nil
	}

	if serverConfig[ZooKeeperRoot] == "" {
		serverConfig[ZooKeeperRoot] = Root
	}

	if serverConfig[ZooKeeperElectionRoot] == "" {
		serverConfig[ZooKeeperElectionRoot] = ElectionRoot
	}

	if serverConfig[ZooKeeperHeartbeatRoot] == "" {
		serverConfig[ZooKeeperHeartbeatRoot] = HeartbeatRoot
	}

	servers := strings.Split(serverConfig[ZooKeeperServers], ";")

	conn, _, err := zk.Connect(servers, 10*time.Second)
	if err != nil {
		glog.Errorf("Failed to create ZooKeeper Connection, error=%s", err)
		return nil
	}

	client := &ZooKeeperClient{
		conn:   conn,
		config: serverConfig,
	}

	go func() {
		if err != nil {
			conn.Close()
		}
	}()

	err = client.mkdirRecursive(ElectionRoot)
	if err != nil {
		return nil
	}

	err = client.mkdirRecursive(HeartbeatRoot)
	if err != nil {
		return nil
	}
	return client
}

// Return GUID strings of the participant in the format of node_%010d
// For e.g, /descartes/election/_c_11f6c4b023b29f33e17d4340ac6815f2-node_0000000010
func (client *ZooKeeperClient) JoinElection(node string) (string, error) {
	fullPath := fmt.Sprintf("%s/%s_", client.config[ZooKeeperElectionRoot], node)
	res, err := client.conn.CreateProtectedEphemeralSequential(fullPath, nil, zk.WorldACL(zk.PermAll))
	if err != nil {
		glog.Errorf("Failed to join client node=%s, error=%s", node, err)
		return "", err
	}
	glog.Infof("%s joined the leader election.", node)
	return res, nil
}

// nodeGUID is the result returned in JoinElection
// Compare all children in the election node, select the one which has min
// sequential number
func (client *ZooKeeperClient) IsLeader(nodeGUID string) (bool, error) {
	participants, err := client.Children(client.config[ZooKeeperElectionRoot])
	if err != nil {
		return false, err
	}

	seqNum, err := strconv.ParseInt(nodeGUID[len(nodeGUID)-10:], 10, 32)
	if err != nil {
		glog.Errorf("Failed to parse %s to int", nodeGUID[len(nodeGUID)-10:])
		return false, err
	}

	var otherParticipants []int32
	for _, participant := range participants {
		num, err := strconv.ParseInt(participant[len(participant)-10:], 10, 32)
		if err != nil {
			glog.Errorf("Failed to parse %s to int", participant[len(participant)-10:])
			return false, err
		}
		otherParticipants = append(otherParticipants, int32(num))
	}

	// FIXME overfollow and rejoin
	for _, num := range otherParticipants {
		if num < int32(seqNum) {
			return false, nil
		}
	}
	return true, nil
}

func (client *ZooKeeperClient) ElectionParticipants() ([]string, error) {
	return client.Children(client.config[ZooKeeperElectionRoot])
}

func (client *ZooKeeperClient) WatchElectionParticipants() (<-chan zk.Event, error) {
	return client.ChildrenW(client.config[ZooKeeperElectionRoot])
}

func (client *ZooKeeperClient) Children(parentNode string) ([]string, error) {
	children, _, err := client.conn.Children(parentNode)
	if err != nil {
		return nil, err
	}
	return children, nil
}

// ChildrenW returns a channel which can receive events when something happened
// to the children
func (client *ZooKeeperClient) ChildrenW(parentNode string) (<-chan zk.Event, error) {
	_, _, eventChan, err := client.conn.ChildrenW(parentNode)
	return eventChan, err
}

func (client *ZooKeeperClient) Close() {
	client.conn.Close()
}

func (client *ZooKeeperClient) DeleteNode(node string) error {
	_, stat, err := client.conn.Get(node)
	if err != nil {
		glog.Errorf("Failed to get node=%s", node)
		return err
	}
	return client.conn.Delete(node, stat.Version)
}

// MkdirAll creates a directory recursively
func (client *ZooKeeperClient) mkdirRecursive(node string) error {
	parent := path.Dir(node)
	if parent != "/" {
		if err := client.mkdirRecursive(parent); err != nil {
			glog.Errorf("Failed to create node=%s", parent)
			return err
		}
	}

	_, err := client.conn.Create(node, nil, 0, zk.WorldACL(zk.PermAll))
	if err == zk.ErrNodeExists {
		return nil
	}
	return err
}

// Create stores a new value at node. Fails if already set
func (client *ZooKeeperClient) CreateNode(node string, value []byte, ephemeral, ignoreExists bool) error {
	if err := client.mkdirRecursive(path.Dir(node)); err != nil {
		glog.Errorf("Failed to create node=%s", path.Dir(node))
		return err
	}

	flags := int32(0)
	if ephemeral {
		flags = zk.FlagEphemeral
	}
	_, err := client.conn.Create(node, value, flags, zk.WorldACL(zk.PermAll))
	if err == nil || (err == zk.ErrNodeExists && ignoreExists) {
		return nil
	}
	glog.Errorf("Failed to create node=%s, error=%s", node, err)
	return err
}

// NodeExists check if the node already exists
func (client *ZooKeeperClient) NodeExists(node string) (bool, error) {
	res, _, err := client.conn.Exists(node)
	return res, err
}
