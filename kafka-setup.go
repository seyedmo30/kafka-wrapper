package kafkawrapper

import (
	"time"

	kafkaPachage "github.com/segmentio/kafka-go"
)

type kafkaConsumer struct {
	socket  string
	topic   string
	groupID string
}

func KafkaConsumerSetup(Socket string, Topic string, GroupID string) kafkaConsumer {

	return kafkaConsumer{socket: Socket, topic: Topic, groupID: GroupID}

}

type kafkaReader struct {
	KafkaReader *kafkaPachage.Reader
}

func (k kafkaConsumer) consumerConnection() (kafkaReader, error) {
	kafkareader := kafkaReader{}
	// const partition = 0

	dialer := &kafkaPachage.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
	}

	// Attempt to dial the Kafka broker
	connDail, err := dialer.Dial("tcp", k.socket)
	if err != nil {
		logs().Error("Failed to connect to Kafka: \n" + err.Error())
		return kafkareader, err
	}

	connDail.Close()
	conn := kafkaPachage.NewReader(kafkaPachage.ReaderConfig{
		Brokers: []string{k.socket},
		GroupID: k.groupID,
		// Partition: partition,
		Topic:    k.topic,
		MaxBytes: 10e6, // 10MB
	})

	kafkareader.KafkaReader = conn
	return kafkareader, nil
}
