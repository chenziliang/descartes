package splunk


import (
	"testing"
	db "github.com/chenziliang/descartes/base"
)


func TestSplunkEventWriter(t *testing.T) {
	serverURL := "https://localhost:8089"
	sinkConfig := []*db.BaseConfig {
		&db.BaseConfig {
		    ServerURL: serverURL,
		    Username: "admin",
	        Password: "admin",
		},
	}

	writer := NewSplunkEventWriter(sinkConfig)
	writer.Start()
	writer.Start()
	defer writer.Stop()

	metaInfo := map[string]string {
		hostKey: "ghost.com",
		indexKey: "main",
		sourceKey: "descartes",
		sourcetypeKey: "my:event:test",
	}

	asyncData := [][]byte {
		[]byte("async:a=b,c=d,e=f,g=h"),
		[]byte("async:1=2,3=4,5=6,7=8"),
	}
	asyncEvent := db.NewEvent(metaInfo, asyncData)
	err := writer.WriteEventsAsync(asyncEvent)
	if err != nil {
		t.Errorf("Failed to index data from %s, error=%s", serverURL, err)
	}

	syncData := [][]byte {
		[]byte("sync:a=b,c=d,e=f,g=h"),
		[]byte("sync:1=2,3=4,5=6,7=8"),
	}
	syncEvent := db.NewEvent(metaInfo, syncData)
	err = writer.WriteEventsSync(syncEvent)
	if err != nil {
		t.Errorf("Failed to index data from %s, error=%s", serverURL, err)
	}

	writer.Stop()
}
