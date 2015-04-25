package snow

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/chenziliang/descartes/base"
	"github.com/golang/glog"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

type collectionState struct {
	Version         string
	NextRecordTime  string
	LastTimeRecords []string
}

type SnowDataReader struct {
	config      base.BaseConfig
	writer      base.DataWriter
	checkpoint  base.Checkpointer
	http_client *http.Client
	state       collectionState
	collecting  int32
	started     int32
}

const (
	endpointKey       = "Endpoint"
	timestampFieldKey = "TimestampField"
	nextRecordTimeKey = "NextRecordTime"
	recordCountKey    = "RecordCount"
	timeTemplate      = "2006-01-02 15:04:05"
)

// NewSnowDataReader
// @config: shall contain snow "ServerURL", "Username", "Password" "Endpoint", "TimestampField"
// "NextRecordTime", "RecordCount" key/values
func NewSnowDataReader(
	config base.BaseConfig, writer base.DataWriter, checkpoint base.Checkpointer) *SnowDataReader {
	acquiredConfigs := []string{base.ServerURL, base.Username, base.Password,
		endpointKey, timestampFieldKey, nextRecordTimeKey, recordCountKey}
	for _, key := range acquiredConfigs {
		if _, ok := config[key]; !ok {
			glog.Errorf("%s is missing. It is required by Snow data collection", key)
			return nil
		}
	}

	state := getCheckpoint(checkpoint, config)
	if state == nil {
		return nil
	}

	return &SnowDataReader{
		config:      config,
		writer:      writer,
		checkpoint:  checkpoint,
		http_client: &http.Client{Timeout: 120 * time.Second},
		state:       *state,
		collecting:  0,
		started:     0,
	}
}

func (snow *SnowDataReader) Start() {
	if !atomic.CompareAndSwapInt32(&snow.started, 0, 1) {
		glog.Infof("SnowDataReader already started")
		return
	}

	snow.writer.Start()
	snow.checkpoint.Start()
	glog.Infof("SnowDataReader started...")
}

func (snow *SnowDataReader) Stop() {
	if !atomic.CompareAndSwapInt32(&snow.started, 1, 0) {
		glog.Infof("SnowDataReader already stopped")
		return
	}

	snow.writer.Stop()
	snow.checkpoint.Stop()
	glog.Infof("SnowDataReader stopped...")
}

func (snow *SnowDataReader) getURL() string {
	nextRecordTime := snow.getNextRecordTime()
	var buffer bytes.Buffer
	buffer.WriteString(snow.config[base.ServerURL])
	buffer.WriteString("/")
	buffer.WriteString(snow.config[endpointKey])
	buffer.WriteString(".do?JSONv2&sysparm_query=")
	buffer.WriteString(snow.config[timestampFieldKey])
	buffer.WriteString(">=")
	buffer.WriteString(nextRecordTime)
	buffer.WriteString("^ORDERBY")
	buffer.WriteString(snow.config[timestampFieldKey])
	buffer.WriteString("&sysparm_record_count=" + snow.config[recordCountKey])
	return buffer.String()
}

func (snow *SnowDataReader) ReadData() ([]byte, error) {
	if !atomic.CompareAndSwapInt32(&snow.collecting, 0, 1) {
		glog.Infof("Last data collection for %s has not been done", snow.getURL())
		return nil, nil
	}
	defer atomic.StoreInt32(&snow.collecting, 0)

	// glog.Infof(snow.getURL())
	req, err := http.NewRequest("GET", snow.getURL(), nil)
	if err != nil {
		glog.Errorf("Failed to create request, error=%s", err)
		return nil, err
	}

	req.Header.Add("Accept-Encoding", "gzip")
	req.Header.Add("Accept", "application/json")
	req.SetBasicAuth(snow.config[base.Username], snow.config[base.Password])

	resp, err := snow.http_client.Do(req)
	if err != nil {
		glog.Errorf("Failed to do request, error=%s", err)
		return nil, err
	}
	defer resp.Body.Close()

	reader, err := gzip.NewReader(resp.Body)
	if err != nil {
		glog.Errorf("Failed to create gzip reader, error=%s", err)
		return nil, err
	}
	defer reader.Close()

	body, err := ioutil.ReadAll(reader)
	if err != nil {
		glog.Errorf("Failed to read uncompressed data, error=%s", err)
		return nil, err
	}
	return body, nil
}

func (snow *SnowDataReader) IndexData() error {
	data, err := snow.ReadData()
	if data == nil || err != nil {
		return err
	}

	jobj, err := base.ToJsonObject(data)
	if err != nil {
		return err
	}

	if records, ok := jobj["records"].([]interface{}); ok {
		metaInfo := map[string]string{
			base.ServerURL: snow.config[base.ServerURL],
			base.Username:  snow.config[base.Username],
			endpointKey:    snow.config[endpointKey],
		}
		records, refreshed := snow.removeCollectedRecords(records)
		allData := base.NewData(metaInfo, make([][]byte, 1))
		var record []string
		for i := 0; i < len(records); i++ {
			record = record[:0]
			for k, v := range records[i].(map[string]interface{}) {
				record = append(record, fmt.Sprintf(`%s="%s"`, k, v))
			}
			allData.RawData = append(allData.RawData, []byte(strings.Join(record, ",")))
			err := snow.writer.WriteData(allData)
			if err != nil {
				return err
			}
			allData.RawData = allData.RawData[:0]
		}

		if len(records) > 0 {
			return snow.writeCheckpoint(records, refreshed)
		}
	} else if errDesc, ok := jobj["error"]; ok {
		glog.Errorf("Failed to get data from %s, error=%s", snow.getURL(), errDesc)
		return errors.New(fmt.Sprintf("%+v", errDesc))
	}
	return nil
}

func (snow *SnowDataReader) doRemoveRecords(records []interface{}, lastTimeRecords map[string]bool,
	lastRecordTime string) []interface{} {
	var recordsToBeRemoved []string
	var recordsToBeIndexed []interface{}
	timefield := snow.config[timestampFieldKey]

	for i := 0; i < len(records); i++ {
		r, ok := records[i].(map[string]interface{})
		if !ok {
			glog.Errorf("Encount unknown format %+v", records[i])
			continue
		}

		if r[timefield] == lastRecordTime {
			sysId, _ := r["sys_id"].(string)
			if _, ok := lastTimeRecords[sysId]; ok {
				recordsToBeRemoved = append(recordsToBeRemoved, sysId)
			} else {
				recordsToBeIndexed = append(recordsToBeIndexed, r)
			}
		} else {
			recordsToBeIndexed = append(recordsToBeIndexed, r)
		}
	}

	//if len(recordsToBeRemoved) > 0 {
    //		glog.Infof("Last time records: %+v with timestamp=%s. "+
	//			"Remove collected records: %s with the same timestamp",
	//			lastTimeRecords, lastRecordTime, recordsToBeRemoved)
	//}
	return recordsToBeIndexed
}

func (snow *SnowDataReader) removeCollectedRecords(records []interface{}) ([]interface{}, bool) {
	if len(snow.state.LastTimeRecords) == 0 || len(records) == 0 {
		return records, false
	}

	lastTimeRecords := make(map[string]bool, len(snow.state.LastTimeRecords))
	for i := 0; i < len(snow.state.LastTimeRecords); i++ {
		lastTimeRecords[snow.state.LastTimeRecords[i]] = true
	}

	lastRecordTime := snow.state.NextRecordTime
	recordsToBeIndexed := snow.doRemoveRecords(records, lastTimeRecords, lastRecordTime)

	refreshed := false
	recordCount, _ := strconv.Atoi(snow.config[recordCountKey])

	if len(records) == recordCount {
		firstRecord := records[0].(map[string]interface{})
		lastRecord := records[len(records)-1].(map[string]interface{})
		timefield := snow.config[timestampFieldKey]
		if firstRecord[timefield] == lastRecord[timefield] {
			// Run into a rare situtaion that there are more than recordCount
			// records with the same timestamp. If this happens, move forward
			// the NextRecordTime to 1 second, otherwise we are running into
			// infinite loop
			glog.Warningf("%d records with same timestamp=%s rare situation happened", recordCount, lastRecordTime)
			nextRecordTime, err := time.Parse(timeTemplate, lastRecordTime)
			if err != nil {
				glog.Errorf("Failed to parse timestamp %s with template=%s, error=%s", lastRecordTime, timeTemplate, err)
				return nil, false
			}

			nextRecordTime = nextRecordTime.Add(time.Second)
			snow.state.NextRecordTime = nextRecordTime.Format(timeTemplate)
			snow.state.LastTimeRecords = snow.state.LastTimeRecords[:0]
			refreshed = true
			glog.Warning("Progress to NextRecordTimestamp=", snow.state.NextRecordTime)
		}
	}
	return recordsToBeIndexed, refreshed
}

func (snow *SnowDataReader) writeCheckpoint(records []interface{}, refreshed bool) error {
	if len(records) == 0 {
		return nil
	}

	timefield := snow.config[timestampFieldKey]
	lastRecord, _ := records[len(records)-1].(map[string]interface{})
	var maxTimestampRecords []string

	for i := len(records) - 1; i >= 0; i-- {
		r := records[i].(map[string]interface{})
		if r[timefield] == lastRecord[timefield] {
			maxTimestampRecords = append(maxTimestampRecords, r["sys_id"].(string))
		} else {
			break
		}
	}

	currentState := &collectionState{
		Version:         "1",
		NextRecordTime:  lastRecord[timefield].(string),
		LastTimeRecords: maxTimestampRecords,
	}

	data, err := json.Marshal(currentState)
	if err != nil {
		glog.Errorf("Failed to marhsal checkpoint, error=%s", err)
		return err
	}

	err = snow.checkpoint.WriteCheckpoint(snow.config, data)
	if err != nil {
		return err
	}

	if !refreshed {
		snow.state = *currentState
	}
	return nil
}

func (snow *SnowDataReader) getNextRecordTime() string {
	return strings.Replace(snow.state.NextRecordTime, " ", "+", 1)
}

func getCheckpoint(checkpoint base.Checkpointer, config base.BaseConfig) *collectionState {
	glog.Infof("State is not in cache, reload from checkpoint")
	data, err := checkpoint.GetCheckpoint(config)
	if err != nil {
		return nil
	}

	state := collectionState{
		Version:         "1",
		NextRecordTime:  config[nextRecordTimeKey],
		LastTimeRecords: []string{},
	}

	if data != nil {
		err = json.Unmarshal(data, &state)
		if err != nil {
			glog.Errorf("Failed to unmarshal data=%s, doesn't conform colllectionState", string(data))
			return nil
		}
	}

	return &state
}
