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
	ck := NewFileCheckpointer(".", "myapp")
	_, err := ck.GetCheckpoint("abc")
	if err == nil {
		t.Error("GetCheckpoint should error out, but got no error")
	}

	p := person{
		FirstName: "Ken",
		LastName:  "Chen",
		Age:       30,
	}

	marshaled, _ := json.Marshal(p)
	err = ck.WriteCheckpoint("myapp_status", marshaled)
	if err != nil {
		t.Errorf("WriteCheckpoint should have no error, but got error=%v", err)
	}

	err = ck.DeleteCheckpoint("myapp_status")
	if err != nil {
		t.Errorf("DeleteCheckpoint should have no error, but got error=%v", err)
	}
}
