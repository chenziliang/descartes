package splunk


import (
	"testing"
	"net/http"
	"crypto/tls"
	"net/url"
)


func TestSplunkRest(t *testing.T) {
	tr := &http.Transport {
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	client := &http.Client{Transport: tr}
	sr := SplunkRest {
		client: client,
	}
	splunkURI := "https://localhost:8089"

	sessionKey, err := sr.Login(splunkURI, "admin", "admin")
	if err != nil {
		t.Errorf("Failed to get session key from %s, error=%s", splunkURI, err)
	}

	metaProps := url.Values{}
	metaProps.Add("host", "myhost.com")
	metaProps.Add("index", "main")
	metaProps.Add("source", "descartes")
	metaProps.Add("sourcetype", "snow:em_event")
	data := []byte("a=b,c=d,e=f,g=h\n1=2,3=4,5=6,7=8\n")
	err = sr.IndexData(splunkURI, sessionKey, &metaProps, data)
	if err != nil {
		t.Errorf("Failed to index data from %s, error=%s", splunkURI, err)
	}
}
