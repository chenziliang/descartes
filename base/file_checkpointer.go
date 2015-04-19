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

func (ck *FileCheckpointer) Start() {
}

func (ck *FileCheckpointer) Stop() {
}

func (ck *FileCheckpointer) GetCheckpoint(keyInfo map[string]string) ([]byte, error) {
	ckFileName := filepath.Join(ck.fileDir, ck.namespace+"_"+keyInfo["Key"]+checkpointFilePostfix)
	content, err := ioutil.ReadFile(ckFileName)
	if err != nil {
		glog.Errorf("Failed to get checkpoint from %s, error=%s", ckFileName, err)
		return nil, err
	}
	return content, err
}

func (ck *FileCheckpointer) WriteCheckpoint(keyInfo map[string]string, value []byte) error {
	ckFileName := filepath.Join(ck.fileDir, ck.namespace+"_"+keyInfo["Key"]+checkpointFilePostfix)
	err := ioutil.WriteFile(ckFileName, []byte(value), 0644)
	if err != nil {
		glog.Errorf("Failed to write checkpoint to %s, error=%s", ckFileName, err)
	}
	return err
}

func (ck *FileCheckpointer) DeleteCheckpoint(keyInfo map[string]string) error {
	ckFileName := filepath.Join(ck.fileDir, ck.namespace+"_"+keyInfo["Key"]+checkpointFilePostfix)
	err := os.Remove(ckFileName)
	if err != nil {
		glog.Errorf("Failed to remove checkpoint %s, error=%s", ckFileName, err)
	}
	return err
}
