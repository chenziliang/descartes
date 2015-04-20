package splunk

import (
	"github.com/chenziliang/descartes/base"
	"testing"
)

func TestSplunkDataWriter(t *testing.T) {
	serverURL := "https://localhost:8089"
	sinkConfig := []*base.BaseConfig{
		&base.BaseConfig{
			ServerURL: serverURL,
			Username:  "admin",
			Password:  "admin",
		},
	}

	writer := NewSplunkDataWriter(sinkConfig)
	writer.Start()
	writer.Start()
	defer writer.Stop()

	metaInfo := map[string]string{
		hostKey:       "ghost.com",
		indexKey:      "main",
		sourceKey:     "descartes",
		sourcetypeKey: "my:event:test",
	}

	asyncData := [][]byte{
		[]byte("async:a=b,c=d,e=f,g=h"),
		[]byte("async:1=2,3=4,5=6,7=8"),
	}
	data := base.NewData(metaInfo, asyncData)
	err := writer.WriteDataAsync(data)
	if err != nil {
		t.Errorf("Failed to index data from %s, error=%s", serverURL, err)
	}

	syncData := [][]byte{
		[]byte("sync:a=b,c=d,e=f,g=h"),
		[]byte("sync:1=2,3=4,5=6,7=8"),
	}
	data = base.NewData(metaInfo, syncData)
	err = writer.WriteDataSync(data)
	if err != nil {
		t.Errorf("Failed to index data from %s, error=%s", serverURL, err)
	}

	writer.Stop()
}
