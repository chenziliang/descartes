package base

import (
	"encoding/json"
	"github.com/golang/glog"
)

func ToJsonObject(data []byte) (map[string]interface{}, error) {
	var jobj map[string]interface{}
	err := json.Unmarshal(data, &jobj)
	if err != nil {
		glog.Errorf("Failed to unmarshal json content, error=%s", err)
		return nil, err
	}
	return jobj, nil
}
