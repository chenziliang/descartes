package base

import (
	"encoding/json"
	"testing"
)

type dummy struct {
	FirstName string
	LastName  string
	Age       int
}

func TestCassandraCheckpointer(t *testing.T) {
	config := BaseConfig{
		// CassandraCluster: "54.169.104.98:9042;52.74.147.39:9042",
		CassandraSeeds:    "172.16.107.153:9042",
		CassandraKeyspace: "descartes",
		CheckpointTable:   "task_ckpts",
	}

	ck := NewCassandraCheckpointer(config)
	if ck == nil {
		t.Errorf("Failed to create CassandraCheckpointer")
	}

	keyInfo := BaseConfig{
		Key: "43298572195723894fsd",
	}

	// Query
	data, err := ck.GetCheckpoint(keyInfo)
	if data != nil || err != nil {
		t.Errorf("GetCheckpoint should not error out, but got error=%s or data=%s", err, data)
	}

	// Write
	p := dummy{
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

	var d dummy
	err = json.Unmarshal(data, &d)
	if d.FirstName != p.FirstName || d.LastName != p.LastName || d.Age != p.Age {
		t.Errorf("Failed to get checkpoint %+v", d)
	}

	// Update
	p.Age = 40
	marshaled, _ = json.Marshal(p)
	err = ck.WriteCheckpoint(keyInfo, marshaled)
	if err != nil {
		t.Errorf("WriteCheckpoint should have no error, but got error=%v", err)
	}

	data, err = ck.GetCheckpoint(keyInfo)
	if err != nil {
		t.Errorf("GetCheckpoint got error=%s", err)
	}

	err = json.Unmarshal(data, &d)
	if d.FirstName != p.FirstName || d.LastName != p.LastName || d.Age != p.Age {
		t.Errorf("Failed to get checkpoint %+v", d)
	}

	// Delete
	err = ck.DeleteCheckpoint(keyInfo)
	if err != nil {
		t.Errorf("DeleteCheckpoint should have no error, but got error=%v", err)
	}

	data, err = ck.GetCheckpoint(keyInfo)
	if data != nil || err != nil {
		t.Errorf("GetCheckpoint should not error out, but got error=%s or data=%s", err, data)
	}
}
