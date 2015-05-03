package base

import (
	"encoding/json"
	"testing"
)

type person struct {
	FirstName string
	LastName  string
	Age       int
}

func TestFileCheckpointer(t *testing.T) {
	keyInfo := map[string]string{
		CheckpointDir:       ".",
		CheckpointNamespace: "myapp",
		CheckpointKey:       "ck_status",
	}

	ck := NewFileCheckpointer()
	data, err := ck.GetCheckpoint(keyInfo)
	if data != nil || err != nil {
		t.Errorf("GetCheckpoint should not error out, but got error=%s or data=%s", err, data)
	}

	p := person{
		FirstName: "Ken",
		LastName:  "Chen",
		Age:       30,
	}

	marshaled, _ := json.Marshal(p)
	err = ck.WriteCheckpoint(keyInfo, marshaled)
	if err != nil {
		t.Errorf("WriteCheckpoint should have no error, but got error=%v", err)
	}

	data, err = ck.GetCheckpoint(keyInfo)
	if err != nil {
		t.Errorf("GetCheckpoint got error=%s", err)
	}

	var pp person
	err = json.Unmarshal(data, &pp)
	if pp.FirstName != p.FirstName || pp.LastName != p.LastName || pp.Age != p.Age {
		t.Errorf("Failed to get checkpoint %v", pp)
	}

	err = ck.DeleteCheckpoint(keyInfo)
	if err != nil {
		t.Errorf("DeleteCheckpoint should have no error, but got error=%v", err)
	}
}
