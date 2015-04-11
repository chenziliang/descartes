package base

import (
	"os"
	"io/ioutil"
	"path/filepath"
	"github.com/golang/glog"
)

type Checkpointer interface {
	GetCheckpoint(key string) ([]byte, error)
	WriteCheckpoint(key string, value []byte) error
	DeleteCheckpoint(key string) error
}


const (
	checkpointFilePostfix = ".ck"
)

type LocalFileCheckpointer struct {
	fileDir string
	namespace string
}


func NewFileCheckpointer(fileDir, namespace string) Checkpointer {
	return &LocalFileCheckpointer {
		fileDir: fileDir,
		namespace: namespace,
	}
}

func (ck *LocalFileCheckpointer) GetCheckpoint(key string) ([]byte, error) {
	ckFileName := filepath.Join(ck.fileDir, ck.namespace + "_" + key + checkpointFilePostfix)
	content, err := ioutil.ReadFile(ckFileName)
	if err != nil {
		glog.Error("Failed to get checkpoint from ", ckFileName)
		return nil, err
	}
	return content, err
}

func (ck *LocalFileCheckpointer) WriteCheckpoint(key string, value []byte) error {
	ckFileName := filepath.Join(ck.fileDir, ck.namespace + "_" + key + checkpointFilePostfix)
	err := ioutil.WriteFile(ckFileName, []byte(value), 0644)
	if err != nil {
		glog.Error("Failed to write checkpoint to ", ckFileName)
	}
	return err
}

func (ck *LocalFileCheckpointer) DeleteCheckpoint(key string) error {
	ckFileName := filepath.Join(ck.fileDir, ck.namespace + "_" + key + checkpointFilePostfix)
	err := os.Remove(ckFileName)
	if err != nil {
		glog.Error("Failed to remove checkpoint ", ckFileName)
	}
	return err
}
