package kafkawrapper

import (
	"context"

	"time"

	kafkaPachage "github.com/segmentio/kafka-go"
)

type kafkaConsumer struct {
	socket                string
	topic                 string
	groupID               string
	kafkaConsumerinstance kafkaReader
}

func KafkaConsumerSetup(Socket string, Topic string, GroupID string) kafkaConsumer {

	return kafkaConsumer{socket: Socket, topic: Topic, groupID: GroupID}

}

type kafkaPublisher struct {
	socket                 string
	topic                  string
	kafkaPublisherinstance kafkaWriter
}

func KafkaPublisherSetup(Socket string, Topic string) kafkaPublisher {

	return kafkaPublisher{socket: Socket, topic: Topic}

}

type kafkaReader struct {
	KafkaReader *kafkaPachage.Reader
}

type kafkaWriter struct {
	KafkaWriter *kafkaPachage.Writer
}

func (k *kafkaConsumer) getter() kafkaReader {

	return k.kafkaConsumerinstance
}

func (k *kafkaPublisher) getter() kafkaWriter {

	return k.kafkaPublisherinstance
}

func (k *kafkaConsumer) close() error {

	k.getter().KafkaReader.Close()
	return nil

}
func (k *kafkaPublisher) close() error {

	defer k.getter().KafkaWriter.Close()
	// kafkaPachage.ACL
	// kafkaPachage.Client

	// // Set up Kafka client configuration
	// config := kafka.WriterConfig{
	// 	Brokers: []string{"localhost:9092"}, // Kafka broker address
	// }

	// // Create a new Kafka client
	// client := kafka.NewWriter(config)

	// // Create a new Admin client
	// adminClient := kafka.NewAdminClient(&kafka.Dialer{
	// 	Timeout: 10, // Timeout in seconds
	// })

	// // Specify the topic to delete
	// topicToDelete := "topic_name"

	// // Delete the specified topic
	// err := adminClient.DeleteTopics(topicToDelete)
	// if err != nil {
	// 	fmt.Fprintf(os.Stderr, "Error deleting topic: %v\n", err)
	// 	os.Exit(1)
	// }

	// fmt.Printf("Topic '%s' deleted successfully.\n", topicToDelete)

	// // Close the Kafka and Admin clients
	// adminClient.Close()
	// client.Close()

	return nil
}

func (k *kafkaPublisher) setter(ctx context.Context, msg WriteMessageDTO) error {

	return k.kafkaPublisherinstance.KafkaWriter.WriteMessages(ctx, kafkaPachage.Message{
		Key:   msg.Key,
		Value: msg.Value,
	})
}

func (k *kafkaPublisher) publisherConnection() error {

	dialer := &kafkaPachage.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
	}

	// Attempt to dial the Kafka broker
	connDail, err := dialer.Dial("tcp", k.socket)
	if err != nil {
		logs().Error("Failed to connect to Kafka: \n" + err.Error())
		return err
	}

	connDail.Close()
	conn := kafkaPachage.NewWriter(kafkaPachage.WriterConfig{
		Brokers: []string{k.socket},
		// Partition: partition,
		Topic: k.topic,
	})

	k.kafkaPublisherinstance.KafkaWriter = conn

	return nil
}

func (k *kafkaConsumer) consumerConnection() error {

	dialer := &kafkaPachage.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
	}

	// Attempt to dial the Kafka broker
	connDail, err := dialer.Dial("tcp", k.socket)
	if err != nil {
		logs().Error("Failed to connect to Kafka: \n" + err.Error())
		return err
	}

	connDail.Close()
	conn := kafkaPachage.NewReader(kafkaPachage.ReaderConfig{
		Brokers: []string{k.socket},
		GroupID: k.groupID,
		// Partition: partition,
		Topic:    k.topic,
		MaxBytes: 10e6, // 10MB
	})

	k.kafkaConsumerinstance.KafkaReader = conn

	return nil
}

func (k *kafkaConsumer) consumerReconnection() error {
	// TODO
	return nil
}
