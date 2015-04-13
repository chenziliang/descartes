package snow

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	db "github.com/chenziliang/descartes/base"
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

type SnowDataLoader struct {
	*db.BaseConfig
	writer      db.EventWriter
	checkpoint  db.Checkpointer
	http_client *http.Client
	state       collectionState
	ckKey       string
	collecting  int32
}

const (
	endpointKey       = "endpoint"
	timestampFieldKey = "timestampField"
	nextRecordTimeKey = "nextRecordTime"
	recordCountKey    = "recordCount"
	timeTemplate      = "2006-01-02 15:04:05"
)

// NewSnowDataLoader
// @config.AdditionalConfig: shall contain snow "endpoint", "timestampField"
// "nextRecordTime", "recordCount" key/values
func NewSnowDataLoader(
	config *db.BaseConfig, writer db.EventWriter, checkpointer db.Checkpointer) *SnowDataLoader {
	acquiredConfigs := []string{endpointKey, timestampFieldKey, nextRecordTimeKey}
	for _, key := range acquiredConfigs {
		if _, ok := config.AdditionalConfig[key]; !ok {
			glog.Errorf("%s is missing. It is required by Snow data collection", key)
			return nil
		}
	}

	reader := []byte(config.ServerURL)
	var buf bytes.Buffer
	encoder := base64.NewEncoder(base64.StdEncoding, &buf)
	defer encoder.Close()
	encoder.Write(reader)

	return &SnowDataLoader{
		BaseConfig:  config,
		writer:      writer,
		checkpoint:  checkpointer,
		http_client: &http.Client{Timeout: 120 * time.Second},
		ckKey:       buf.String() + "_" + config.AdditionalConfig[endpointKey],
		collecting:  0,
	}
}

func (snow *SnowDataLoader) getURL() string {
	nextRecordTime := snow.getNextRecordTime()
	var buffer bytes.Buffer
	buffer.WriteString(snow.ServerURL)
	buffer.WriteString("/")
	buffer.WriteString(snow.AdditionalConfig[endpointKey])
	buffer.WriteString(".do?JSONv2&sysparm_query=")
	buffer.WriteString(snow.AdditionalConfig[timestampFieldKey])
	buffer.WriteString(">=")
	buffer.WriteString(nextRecordTime)
	buffer.WriteString("^ORDERBY")
	buffer.WriteString(snow.AdditionalConfig[timestampFieldKey])
	buffer.WriteString("&sysparm_record_count=" + snow.AdditionalConfig[recordCountKey])
	return buffer.String()
}

func (snow *SnowDataLoader) CollectData() ([]byte, error) {
	if !atomic.CompareAndSwapInt32(&snow.collecting, 0, 1) {
		glog.Info("Last data collection for %s has not been done", snow.getURL())
		return nil, nil
	}
	defer atomic.StoreInt32(&snow.collecting, 1)

	fmt.Println(snow.getURL())
	req, err := http.NewRequest("GET", snow.getURL(), nil)
	if err != nil {
		glog.Error("Failed to create request, error=", err)
		return nil, err
	}

	req.Header.Add("Accept-Encoding", "gzip")
	req.Header.Add("Accept", "application/json")
	req.SetBasicAuth(snow.Username, snow.Password)

	resp, err := snow.http_client.Do(req)
	if err != nil {
		glog.Error("Failed to do request, error=", err)
		return nil, err
	}
	defer resp.Body.Close()

	reader, err := gzip.NewReader(resp.Body)
	if err != nil {
		glog.Error("Failed to create gzip reader, error=", err)
		return nil, err
	}
	defer reader.Close()

	body, err := ioutil.ReadAll(reader)
	if err != nil {
		glog.Error("Failed to read uncompressed data, error=", err)
		return nil, err
	}
	return body, nil
}

func (snow *SnowDataLoader) IndexData() error {
	data, err := snow.CollectData()
	if err != nil {
		return err
	}

	if data == nil {
		return nil
	}

	jobj, err := db.ToJsonObject(data)
	if err != nil {
		return err
	}

	if records, ok := jobj["records"].([]interface{}); ok {
		// FIXME metaInfo
		metaInfo := map[string]string{}
		records, refreshed := snow.removeCollectedRecords(records)
		allEvents := db.NewEvent(metaInfo, make([][]byte, len(records)))
		var record []string
		for i := 0; i < len(records); i++ {
			record = record[:0]
			for k, v := range records[i].(map[string]interface{}) {
				record = append(record, fmt.Sprintf(`%s="%s"`, k, v))
			}
			allEvents.RawEvents = append(allEvents.RawEvents, []byte(strings.Join(record, ",")))
		}

		if len(records) > 0 {
			err := snow.writer.WriteEvents(allEvents)
			if err != nil {
				return err
			}
			return snow.writeCheckpoint(records, refreshed)
		}
	} else if errDesc, ok := jobj["error"]; ok {
		glog.Errorf("Failed to get data from %s, error=%s", snow.getURL(), errDesc)
		return errors.New(fmt.Sprintf("%+v", errDesc))
	}
	return nil
}

func (snow *SnowDataLoader) doRemoveRecords(records []interface{}, lastTimeRecords map[string]bool, lastRecordTime string) []interface{} {
	var recordsToBeRemoved []string
	var recordsToBeIndexed []interface{}
	timefield := snow.AdditionalConfig[timestampFieldKey]

	for i := 0; i < len(records); i++ {
		r, ok := records[i].(map[string]interface{})
		if !ok {
			glog.Error("Encount unknown format %+v", records[i])
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

	if len(recordsToBeRemoved) > 0 {
		glog.Info("Last time records: %s with timestamp=%s. "+
			"Remove collected records: %s with the same timestamp",
			lastTimeRecords, lastRecordTime, recordsToBeRemoved)
	}
	return recordsToBeIndexed
}

func (snow *SnowDataLoader) removeCollectedRecords(records []interface{}) ([]interface{}, bool) {
	ck := snow.getCheckpoint()
	// FIXME check nullness of ck for error
	if ck == nil || len(ck.LastTimeRecords) == 0 || len(records) == 0 {
		return records, false
	}

	lastTimeRecords := make(map[string]bool, len(ck.LastTimeRecords))
	for i := 0; i < len(ck.LastTimeRecords); i++ {
		lastTimeRecords[ck.LastTimeRecords[i]] = true
	}

	lastRecordTime := ck.NextRecordTime
	recordsToBeIndexed := snow.doRemoveRecords(records, lastTimeRecords, lastRecordTime)

	refreshed := false
	recordCount, _ := strconv.Atoi(snow.AdditionalConfig[recordCountKey])

	if len(records) == recordCount {
		firstRecord := records[0].(map[string]interface{})
		lastRecord := records[len(records)-1].(map[string]interface{})
		timefield := snow.AdditionalConfig[timestampFieldKey]
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

func (snow *SnowDataLoader) writeCheckpoint(records []interface{}, refreshed bool) error {
	if len(records) == 0 {
		return nil
	}

	timefield := snow.AdditionalConfig[timestampFieldKey]
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
		glog.Error("Failed to marhsal checkpoint, error=", err)
		return err
	}

	err = snow.checkpoint.WriteCheckpoint(snow.ckKey, data)
	if err != nil {
		return err
	}

	if !refreshed {
		snow.state = *currentState
	}
	return nil
}

func (snow *SnowDataLoader) getCheckpoint() *collectionState {
	if snow.state.NextRecordTime != "" {
		return &snow.state
	}

	glog.Info("State is not in cache, reload from checkpoint")
	ck, err := snow.checkpoint.GetCheckpoint(snow.ckKey)
	if err != nil {
		return nil
	}

	var currentState collectionState
	err = json.Unmarshal(ck, &currentState)
	if err != nil {
		glog.Error("Failed to unmarshal checkpoint, error=", err)
		return nil
	}

	glog.Info("Checkpoint found, populate cache")
	snow.state = currentState

	return &currentState
}

func (snow *SnowDataLoader) getNextRecordTime() string {
	state := snow.getCheckpoint()
	if state == nil {
		glog.Info("Checkpoint not found, use intial configuration")
		snow.state.NextRecordTime = snow.AdditionalConfig["nextTimestamp"]
	}
	return strings.Replace(snow.state.NextRecordTime, " ", "+", 1)
}
