package base

import (
	"errors"
	"github.com/gocql/gocql"
	"github.com/golang/glog"
	"strconv"
	"strings"
	"time"
)

type CassandraCheckpointer struct {
	config  BaseConfig
	cluster *gocql.ClusterConfig
}

func NewCassandraCheckpointer(config BaseConfig) *CassandraCheckpointer {
	for _, required := range []string{CassandraSeeds, CassandraKeyspace, CheckpointTable} {
		if val, ok := config[required]; !ok || val == "" {
			glog.Errorf("Missing %s in the config", required)
			return nil
		}
	}

	var ips []string
	var port string
	for _, ipPort := range strings.Split(config[CassandraSeeds], ";") {
		if idx := strings.Index(ipPort, ":"); idx > 0 && idx < len(ipPort)-1 {
			ips = append(ips, ipPort[:idx])
			port = ipPort[idx+1:]
		}
	}

	portNo, err := strconv.ParseInt(port, 10, 32)
	if err != nil {
		glog.Errorf("Invalid port=%s", port)
		return nil
	}

	cluster := gocql.NewCluster(ips...)
	cluster.Keyspace = config[CassandraKeyspace]
	// cluster.Consistency = gocql.Quorum
	cluster.Consistency = gocql.One
	cluster.Timeout = 30 * time.Second
	cluster.Port = int(portNo)

	return &CassandraCheckpointer{
		config:  config,
		cluster: cluster,
	}
}

func (checkpoint *CassandraCheckpointer) Start() {
}

func (checkpoint *CassandraCheckpointer) Stop() {
}

// @keyInfo: shall contain a Key
func (checkpoint *CassandraCheckpointer) GetCheckpoint(keyInfo map[string]string) ([]byte, error) {
	if k, ok := keyInfo[Key]; !ok || k == "" {
		return nil, errors.New("Missing Key in the config")
	}

	session, err := checkpoint.cluster.CreateSession()
	if err != nil {
		glog.Errorf("Failed to create Cassandra session, error=%s", err)
		return nil, err
	}
	defer session.Close()

	var ckpt []byte
	statement := `SELECT ckpt from ` + checkpoint.config[CheckpointTable] + ` where key = ?`
	err = session.Query(statement, keyInfo[Key]).Scan(&ckpt)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			return nil, nil
		}
		glog.Errorf("Failed to get ckpt for key=%s, error=%s", keyInfo[Key], err)
		return nil, err
	}

	return ckpt, nil
}

// @keyInfo: shall contain a Key
func (checkpoint *CassandraCheckpointer) WriteCheckpoint(keyInfo map[string]string, value []byte) error {
	if k, ok := keyInfo[Key]; !ok || k == "" {
		return errors.New("Missing Key in the config")
	}

	session, err := checkpoint.cluster.CreateSession()
	if err != nil {
		glog.Errorf("Failed to create Cassandra session, error=%s", err)
		return err
	}
	defer session.Close()

	statement := `INSERT INTO ` + checkpoint.config[CheckpointTable] + ` (key, ckpt) VALUES (?, ?)`
	err = session.Query(statement, keyInfo[Key], value).Exec()
	if err != nil {
		glog.Errorf("Failed to write ckpt for key=%s, error=%s", keyInfo[Key], err)
		return err
	}

	return nil
}

func (checkpoint *CassandraCheckpointer) DeleteCheckpoint(keyInfo map[string]string) error {
	if k, ok := keyInfo[Key]; !ok || k == "" {
		return errors.New("Missing Key in the config")
	}

	session, err := checkpoint.cluster.CreateSession()
	if err != nil {
		glog.Errorf("Failed to create Cassandra session, error=%s", err)
		return err
	}
	defer session.Close()

	statement := `DELETE FROM ` + checkpoint.config[CheckpointTable] + ` WHERE key =  ?`
	err = session.Query(statement, keyInfo[Key]).Exec()
	if err != nil {
		glog.Errorf("Failed to delete ckpt for key=%s, error=%s", keyInfo[Key], err)
		return err
	}

	return nil
}
