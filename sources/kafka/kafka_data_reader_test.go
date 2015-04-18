package kafka

import (
	_ "fmt"
	_ "github.com/Shopify/sarama"
	db "github.com/chenziliang/descartes/base"
	"testing"
	"time"
)

func TestKafkaDataReader(t *testing.T) {
	brokerConfigs := []*db.BaseConfig{
		&db.BaseConfig{
			ServerURL: "172.16.107.153:9092",
		},
	}
	client := db.NewKafkaClient(brokerConfigs, "consumerClient")

	consumerGroup, topic := "testConsumerGroup", "DescartesTest"
	var partition int32 = 0
	readerConfig := KafkaDataReaderConfig{
		ConsumerGroup: consumerGroup,
		Topic:         topic,
		Partition:     partition,
	}

	writer := &db.StdoutDataWriter{}
	ck := db.NewFileCheckpointer(".", "test")
	writer.Start()
	dataReader := NewKafkaDataReader(client, readerConfig, writer, ck)
	go dataReader.IndexData()
	time.Sleep(3 * time.Second)
	dataReader.Stop()

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
	fmt.Printf("offset=%d, error=%s\n", offset, err)*/

	time.Sleep(5 * time.Second)
}
