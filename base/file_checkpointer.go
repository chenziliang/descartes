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
	fileDir   string
	namespace string
}

func NewFileCheckpointer(fileDir, namespace string) Checkpointer {
	return &FileCheckpointer{
		fileDir:   fileDir,
		namespace: namespace,
	}
}

func (ck *FileCheckpointer) GetCheckpoint(keyInfo map[string]string) ([]byte, error) {
	ckFileName := filepath.Join(ck.fileDir, ck.namespace+"_"+keyInfo["key"]+checkpointFilePostfix)
	content, err := ioutil.ReadFile(ckFileName)
	if err != nil {
		glog.Error("Failed to get checkpoint from ", ckFileName)
		return nil, err
	}
	return content, err
}

func (ck *FileCheckpointer) WriteCheckpoint(keyInfo map[string]string, value []byte) error {
	ckFileName := filepath.Join(ck.fileDir, ck.namespace+"_"+keyInfo["key"]+checkpointFilePostfix)
	err := ioutil.WriteFile(ckFileName, []byte(value), 0644)
	if err != nil {
		glog.Error("Failed to write checkpoint to ", ckFileName)
	}
	return err
}

func (ck *FileCheckpointer) DeleteCheckpoint(keyInfo map[string]string) error {
	ckFileName := filepath.Join(ck.fileDir, ck.namespace+"_"+keyInfo["key"]+checkpointFilePostfix)
	err := os.Remove(ckFileName)
	if err != nil {
		glog.Error("Failed to remove checkpoint ", ckFileName)
	}
	return err
}
