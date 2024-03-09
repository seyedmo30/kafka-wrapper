package kafkawrapper

import (
	"context"
	"log/slog"
	"time"

	kafkaPachage "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol"
)

// kafkaConsumer represents a Kafka consumer.
type kafkaConsumer struct {
	socket                string              // Kafka broker address
	topic                 string              // Topic to consume messages from
	groupID               string              // Consumer group ID
	kafkaConsumerinstance kafkaPachage.Reader // Kafka reader instance
}

// KafkaConsumerSetup initializes and returns a new KafkaConsumer instance.
func KafkaConsumerSetup(Socket string, Topic string, GroupID string) kafkaConsumer {
	return kafkaConsumer{socket: Socket, topic: Topic, groupID: GroupID}
}

// kafkaPublisher represents a Kafka publisher.
type kafkaPublisher struct {
	socket                 string              // Kafka broker address
	topic                  string              // Topic to publish messages to
	kafkaPublisherinstance kafkaPachage.Writer // Kafka writer instance
}

// KafkaPublisherSetup initializes and returns a new KafkaPublisher instance.
func KafkaPublisherSetup(Socket string, Topic string) kafkaPublisher {
	return kafkaPublisher{socket: Socket, topic: Topic}
}

// getter reads a message from Kafka.
func (k *kafkaConsumer) getter(ctx context.Context) (ReadMessageDTO, error) {
	msg, err := k.kafkaConsumerinstance.ReadMessage(ctx)
	if err == nil {
		err = k.kafkaConsumerinstance.CommitMessages(ctx, msg)
		if err != nil {
			logger.Error("kafka cant commit msg", "key", msg.Key)
		}

	}
	headers := make([]Header, 0, 1)
	for _, header := range msg.Headers {
		headers = append(headers, Header{Key: header.Key, Value: header.Value})

	}

	return ReadMessageDTO{Key: msg.Key, Value: msg.Value, Headers: headers}, err
}

// close closes the KafkaConsumer instance.
func (k *kafkaConsumer) close() error {
	slog.Info("before close Consumer connection")
	return k.kafkaConsumerinstance.Close()
}

// close closes the KafkaPublisher instance.
func (k *kafkaPublisher) close() error {
	slog.Info("before close Publisher connection")
	return k.kafkaPublisherinstance.Close()
}

// setter writes a message to Kafka.
func (k *kafkaPublisher) setter(ctx context.Context, msg WriteMessageDTO) error {

	headers := make([]protocol.Header, len(msg.Headers))
	for _, v := range msg.Headers {
		headers = append(headers, protocol.Header{Key: v.Key, Value: v.Value})
	}
	slog.Debug("before WriteMessages", "Key : ", msg.Key)

	return k.kafkaPublisherinstance.WriteMessages(ctx, kafkaPachage.Message{
		Key:     msg.Key,
		Value:   msg.Value,
		Headers: headers,
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
		logger.Error("Failed to connect to publisher Kafka  ", "external_error", err.Error())
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
		logger.Error("Failed to connect to consumer Kafka  ", "external_error", err.Error())

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
