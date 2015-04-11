package splunk


import (
	"testing"
	db "github.com/chenziliang/descartes/base"
)


func TestSplunkEventWriter(t *testing.T) {
	sinkConfig := []*db.BaseConfig {
		&db.BaseConfig {
		    ServerURL: "https://localhost:8089",
		    Username: "admin",
	        Password: "admin",
		},
	}

	_ = NewSplunkEventWriter(sinkConfig)
}

