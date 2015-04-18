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

func TestCheckpointer(t *testing.T) {
	keyInfo := map[string]string{
		"key": "myapp_status",
	}

	ck := NewFileCheckpointer(".", "myapp")
	_, err := ck.GetCheckpoint(keyInfo)
	if err == nil {
		t.Error("GetCheckpoint should error out, but got no error")
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

	data, err := ck.GetCheckpoint(keyInfo)
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
