package kafka

import (
	_ "encoding/json"
	_ "fmt"
	_ "github.com/Shopify/sarama"
	"github.com/chenziliang/descartes/base"
	"testing"
	"time"
)

func doTest() {
	/*keyInfo := map[string]string {
		"Topic": readerConfig.CheckpointTopic,
		"Partition": fmt.Sprintf("%d", readerConfig.CheckpointPartition),
	}
	fmt.Println("%+v", keyInfo)
	ck := base.NewKafkaCheckpointer(client)
	var state collectionState
	sdata, _ := json.Marshal(&state)
	err := ck.WriteCheckpoint(keyInfo, sdata)
	fmt.Println("%s", err)
	data, err := ck.GetCheckpoint(keyInfo)
	fmt.Println(string(data))
	fmt.Println(err)*/

	/*broker := sarama.NewBroker("172.16.107.153:9092")
	config := sarama.NewConfig()
	broker.Open(config)
	req := &sarama.ConsumerMetadataRequest {
		ConsumerGroup: consumerGroup,
	}
	resp, err := broker.GetConsumerMetadata(req)
	if err != nil {
		t.Errorf("Failed to get consumer meta data, error=%s", err)
	}

	fmt.Printf("origin broker id=%d\n", broker.ID())
	fmt.Printf("coid=%d, coaddr=%s\n", resp.Coordinator.ID() , resp.Coordinator.Addr())
	err = resp.Coordinator.Open(sarama.NewConfig())
	if err != nil {
		fmt.Printf("Failed to open broker, error=%s", err)
	}

	creq := &sarama.OffsetCommitRequest {
		ConsumerGroup: consumerGroup,
		ConsumerGroupGeneration: 0,
		ConsumerID: "testConsumer",
		RetentionTime: int64(time.Hour * 10),
		Version: 1,
	}
	offset := int64(13)
	creq.AddBlock(topic, partition, offset, time.Now().UnixNano(), "mymeta")
	fmt.Printf("Commit offset for consumergroup=%s, topic=%s, partition=%d, offset=%d\n", consumerGroup, topic, partition, offset)
	_, err = resp.Coordinator.CommitOffset(creq)
	if err != nil {
		fmt.Printf("Failed to open broker, error=%s", err)
	}

	freq := &sarama.OffsetFetchRequest {
		ConsumerGroup: consumerGroup,
		Version: 1,
	}
	freq.AddPartition(topic, partition)
	fresp, err := resp.Coordinator.FetchOffset(freq)
	if err != nil {
		t.Errorf("Failed to get offset, error=%s", err)
	}
	fmt.Printf("Get offset for ")
	for k, v := range fresp.Blocks {
		fmt.Printf("topic=%s ", k)
		for kk, vv := range v {
			fmt.Printf("partition=%d, offset=%d\n", kk, vv.Offset)
		}
	}

	ofreq := &sarama.OffsetRequest {
	}
	ofreq.AddBlock(topic, partition, time.Now().UnixNano(), 10)
	oresp, err := resp.Coordinator.GetAvailableOffsets(ofreq)
	fmt.Printf("offsets=%+v\n", oresp.GetBlock(topic, partition).Offsets)

	//consumerGroup = "xxxx"
	offset, err = client.GetPartitionOffset(consumerGroup, topic, partition)
	fmt.Printf("offset=%d, error=%s\n", offset, err)

	offset, err := client.GetProducerOffset(topic, partition)
	fmt.Println(offset)

	data, err := client.GetLastBlock(topic, partition)
	if err != nil {
		t.Errorf("Failed to get last block, error=%s", err)
	}
	fmt.Println(string(data)) */
}

func TestKafkaDataReader(t *testing.T) {
	brokerConfig := base.BaseConfig{
		base.Brokers: "172.16.107.153:9092",
	}
	client := base.NewKafkaClient(brokerConfig, "consumerClient")
	if client == nil {
		t.Errorf("Failed to create KafkaClient")
	}

	consumerGroup, topic := "testConsumerGroup", "DescartesTest"
	ckTopic := "CheckpointTopic_1"
	config := map[string]string{
		base.ConsumerGroup:       consumerGroup,
		base.Topic:               topic,
		base.Partition:           "0",
		base.CheckpointTopic:     ckTopic,
		base.CheckpointPartition: "0",
	}

	writer := &base.StdoutDataWriter{}
	writer.Start()

	ck := base.NewKafkaCheckpointer(client)
	ck.Start()

	dataReader := NewKafkaDataReader(client, config, writer, ck)
	if dataReader == nil {
		t.Errorf("Failed to create KafkaDataReader")
	}
	dataReader.Start()

	go dataReader.IndexData()

	time.Sleep(20 * time.Second)

	dataReader.Stop()
	ck.Stop()
	writer.Stop()

	time.Sleep(time.Second)
}
