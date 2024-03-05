package kafkawrapper

import (
	"context"
	"time"

	kafkaPachage "github.com/segmentio/kafka-go"
)

// kafkaConsumer represents a Kafka consumer.
type kafkaConsumer struct {
	socket                string                   // Kafka broker address
	topic                 string                   // Topic to consume messages from
	groupID               string                   // Consumer group ID
	kafkaConsumerinstance kafkaPachage.Reader     // Kafka reader instance
}

// KafkaConsumerSetup initializes and returns a new KafkaConsumer instance.
func KafkaConsumerSetup(Socket string, Topic string, GroupID string) kafkaConsumer {
	return kafkaConsumer{socket: Socket, topic: Topic, groupID: GroupID}
}

// kafkaPublisher represents a Kafka publisher.
type kafkaPublisher struct {
	socket                 string                   // Kafka broker address
	topic                  string                   // Topic to publish messages to
	kafkaPublisherinstance kafkaPachage.Writer     // Kafka writer instance
}

// KafkaPublisherSetup initializes and returns a new KafkaPublisher instance.
func KafkaPublisherSetup(Socket string, Topic string) kafkaPublisher {
	return kafkaPublisher{socket: Socket, topic: Topic}
}

// getter reads a message from Kafka.
func (k *kafkaConsumer) getter(ctx context.Context) (ReadMessageDTO, error) {
	msg, err := k.kafkaConsumerinstance.ReadMessage(ctx)
	return ReadMessageDTO{Key: msg.Key, Value: msg.Value}, err
}

// close closes the KafkaConsumer instance.
func (k *kafkaConsumer) close() error {
	k.kafkaConsumerinstance.Close()
	return nil
}

// close closes the KafkaPublisher instance.
func (k *kafkaPublisher) close() error {
	k.kafkaPublisherinstance.Close()
	return nil
}

// setter writes a message to Kafka.
func (k *kafkaPublisher) setter(ctx context.Context, msg WriteMessageDTO) error {
	return k.kafkaPublisherinstance.WriteMessages(ctx, kafkaPachage.Message{
		Key:   msg.Key,
		Value: msg.Value,
	})
}

// publisherConnection establishes a connection to Kafka for publishing messages.
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

	// Close the connection after dialing
	connDail.Close()

	// Create a new Kafka writer instance
	conn := kafkaPachage.NewWriter(kafkaPachage.WriterConfig{
		Brokers: []string{k.socket},
		Topic:   k.topic,
	})

	// Assign the Kafka writer instance to the kafkaPublisherinstance
	k.kafkaPublisherinstance = *conn

	return nil
}

// consumerConnection establishes a connection to Kafka for consuming messages.
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

	// Close the connection after dialing
	connDail.Close()

	// Create a new Kafka reader instance
	conn := kafkaPachage.NewReader(kafkaPachage.ReaderConfig{
		Brokers:  []string{k.socket},
		GroupID:  k.groupID,
		Topic:    k.topic,
		MaxBytes: 10e6, // 10MB
	})

	// Assign the Kafka reader instance to the kafkaConsumerinstance
	k.kafkaConsumerinstance = *conn

	return nil
}
