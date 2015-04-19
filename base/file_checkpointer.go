package base

import (
	"github.com/golang/glog"
	"io/ioutil"
	"os"
	"path/filepath"
)

const (
	checkpointFilePostfix = ".ck"
)

type FileCheckpointer struct {
}

func NewFileCheckpointer() Checkpointer {
	return &FileCheckpointer{}
}

func (ck *FileCheckpointer) Start() {
}

func (ck *FileCheckpointer) Stop() {
}

// @keyInfo: contains "CheckpointDir", "CheckpointNamespace", "CheckpointKey"
func (ck *FileCheckpointer) GetCheckpoint(keyInfo map[string]string) ([]byte, error) {
	ckFileName := filepath.Join(keyInfo[CheckpointDir], keyInfo[CheckpointNamespace]+"_"+keyInfo[CheckpointKey]+checkpointFilePostfix)
	content, err := ioutil.ReadFile(ckFileName)
	if err != nil {
		glog.Errorf("Failed to get checkpoint from %s, error=%s", ckFileName, err)
		return nil, err
	}
	return content, err
}

// @keyInfo: contains "CheckpointDir", "CheckpointNamespace", "CheckpointKey"
func (ck *FileCheckpointer) WriteCheckpoint(keyInfo map[string]string, value []byte) error {
	ckFileName := filepath.Join(keyInfo[CheckpointDir], keyInfo[CheckpointNamespace]+"_"+keyInfo[CheckpointKey]+checkpointFilePostfix)
	err := ioutil.WriteFile(ckFileName, []byte(value), 0644)
	if err != nil {
		glog.Errorf("Failed to write checkpoint to %s, error=%s", ckFileName, err)
	}
	return err
}

// @keyInfo: contains "CheckpointDir", "CheckpointNamespace", "CheckpointKey"
func (ck *FileCheckpointer) DeleteCheckpoint(keyInfo map[string]string) error {
	ckFileName := filepath.Join(keyInfo[CheckpointDir], keyInfo[CheckpointNamespace]+"_"+keyInfo[CheckpointKey]+checkpointFilePostfix)
	err := os.Remove(ckFileName)
	if err != nil {
		glog.Errorf("Failed to remove checkpoint %s, error=%s", ckFileName, err)
	}
	return err
}
