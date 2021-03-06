package splunk

import (
	"bytes"
	"encoding/xml"
	"github.com/golang/glog"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
)

type SplunkRest struct {
	client *http.Client
}

// IndexData:
// @metaProps: contains "host", "host_regex", "index", "source",
//             "sourcetype" key/values
func (rest SplunkRest) IndexData(splunkdURI string, sessionKey string,
	metaProps *url.Values, data []byte) error {
	uri := splunkdURI + "/services/receivers/simple?" + metaProps.Encode()
	_, err := rest.SplunkdRequest(uri, sessionKey, "POST", nil, data, 3)
	return err
}

func (rest SplunkRest) addHeaders(req *http.Request, headers map[string]string, sessionKey string) {
	for k, v := range headers {
		req.Header.Add(k, v)
	}

	hasContentType := false
	for _, ct := range []string{"Content-Type", "content-type"} {
		if _, ok := headers[ct]; ok {
			hasContentType = true
			break
		}
	}

	if !hasContentType {
		req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	}

	req.Header.Add("Authorization", "Splunk "+sessionKey)
}

func (rest SplunkRest) SplunkdRequest(splunkdURI string, sessionKey string,
	method string, headers map[string]string, data []byte, retry int) ([]byte, error) {
	var res []byte = nil
	var err error = nil
	for i := 0; i < retry; i++ {
		var reader io.Reader
		if data != nil {
			reader = bytes.NewBuffer(data)
		}

		req, err := http.NewRequest(method, splunkdURI, reader)
		if err != nil {
			glog.Errorf("Failed to create request to %s, reason=%s", splunkdURI, err)
			return nil, err
		}

		rest.addHeaders(req, headers, sessionKey)
		resp, err := rest.client.Do(req)
		if err != nil {
			glog.Errorf("Failed to %s to %s, error=%s", method, splunkdURI, err)
			return nil, err
		}
		defer resp.Body.Close()
		return ioutil.ReadAll(resp.Body)
	}
	return res, err
}

func (rest SplunkRest) Login(splunkdURI, username, password string) (string, error) {
	data := url.Values{}
	data.Add("username", username)
	data.Add("password", password)
	cred := []byte(data.Encode())
	uri := splunkdURI + "/services/auth/login"
	req, err := http.NewRequest("POST", uri, bytes.NewBuffer(cred))
	if err != nil {
		glog.Errorf("Failed to create request to %s, reason=%s", uri, err)
		return "", err
	}

	resp, err := rest.client.Do(req)
	if err != nil {
		glog.Errorf("Failed to login to %s, error=%s", uri, err)
		return "", err
	}
	defer resp.Body.Close()

	res, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		glog.Errorf("Failed to read response from %s, error=%s", uri, err)
		return "", nil
	}

	type sessionKey struct {
		SessionKey string `xml:"sessionKey"`
	}
	var key sessionKey
	err = xml.Unmarshal(res, &key)
	if err != nil {
		glog.Errorf("Failed to parse login XML response from %s, error=%s", uri, err)
		return "", err
	}

	return key.SessionKey, nil
}
