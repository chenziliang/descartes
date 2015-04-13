package base

import (
	"encoding/json"
	"github.com/golang/glog"
)

func ToJsonObject(data []byte) (map[string]interface{}, error) {
	var jobj map[string]interface{}
	err := json.Unmarshal(data, &jobj)
	if err != nil {
		glog.Error("Failed to unmarshal json content, error=", err)
		return nil, err
	}
	return jobj, nil
}
