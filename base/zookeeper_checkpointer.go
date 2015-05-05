package base

import (
	"errors"
	"github.com/golang/glog"
)

type ZooKeeperCheckpointer struct {
	config BaseConfig
	zkClient *ZooKeeperClient
}

func NewZooKeeperCheckpointer(config BaseConfig) *ZooKeeperCheckpointer {
	client := NewZooKeeperClient(config)
	if client == nil {
		return nil
	}

	return &ZooKeeperCheckpointer{
		config:  config,
		zkClient: client,
	}
}

func (checkpoint *ZooKeeperCheckpointer) Start() {
}

// FIXME, guard the Close
func (checkpoint *ZooKeeperCheckpointer) Stop() {
	checkpoint.zkClient.Close()
}

// @keyInfo: shall contain a Key which is a node path
func (checkpoint *ZooKeeperCheckpointer) GetCheckpoint(keyInfo map[string]string) ([]byte, error) {
	if k, ok := keyInfo[Key]; !ok || k == "" {
		return nil, errors.New("Missing Key in the config")
	}

	data, err := checkpoint.zkClient.GetNode(keyInfo[Key], true)
	if err != nil {
		glog.Errorf("Failed to get ckpt for key=%s, error=%s", keyInfo[Key], err)
		return nil, err
	}

	return data, nil
}

// @keyInfo: shall contain a Key which is a node path
func (checkpoint *ZooKeeperCheckpointer) WriteCheckpoint(keyInfo map[string]string, value []byte) error {
	if k, ok := keyInfo[Key]; !ok || k == "" {
		return errors.New("Missing Key in the config")
	}
	err := checkpoint.zkClient.DeleteNode(keyInfo[Key], true)
	if err != nil {
		glog.Errorf("Failed to delete ckpt for key=%s, error=%s", keyInfo[Key], err)
		return err
	}

	err = checkpoint.zkClient.CreateNode(keyInfo[Key], value, false, false)
	if err != nil {
		glog.Errorf("Failed to write ckpt for key=%s, error=%s", keyInfo[Key], err)
	}
	return err
}

// @keyInfo: shall contain a Key which is a node path
func (checkpoint *ZooKeeperCheckpointer) DeleteCheckpoint(keyInfo map[string]string) error {
	if k, ok := keyInfo[Key]; !ok || k == "" {
		return errors.New("Missing Key in the config")
	}

	err := checkpoint.zkClient.DeleteNode(keyInfo[Key], true)
	if err != nil {
		glog.Errorf("Failed to delete ckpt for key=%s, error=%s", keyInfo[Key], err)
	}

	return err
}
