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
	kafkaConsumerinstance kafkaPachage.Reader
}

func KafkaConsumerSetup(Socket string, Topic string, GroupID string) kafkaConsumer {

	return kafkaConsumer{socket: Socket, topic: Topic, groupID: GroupID}

}

type kafkaPublisher struct {
	socket                 string
	topic                  string
	kafkaPublisherinstance kafkaPachage.Writer
}

func KafkaPublisherSetup(Socket string, Topic string) kafkaPublisher {

	return kafkaPublisher{socket: Socket, topic: Topic}

}

func (k *kafkaConsumer) getter(ctx context.Context) (ReadMessageDTO, error) {
	msg, err := k.kafkaConsumerinstance.ReadMessage(ctx)

	return ReadMessageDTO{Key: msg.Key, Value: msg.Value}, err
}

func (k *kafkaConsumer) close() error {

	k.kafkaConsumerinstance.Close()
	return nil

}
func (k *kafkaPublisher) close() error {

	k.kafkaPublisherinstance.Close()

	return nil
}

func (k *kafkaPublisher) setter(ctx context.Context, msg WriteMessageDTO) error {

	return k.kafkaPublisherinstance.WriteMessages(ctx, kafkaPachage.Message{
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

	k.kafkaPublisherinstance = *conn

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

	k.kafkaConsumerinstance = *conn

	return nil
}

func (k *kafkaConsumer) consumerReconnection() error {
	// TODO
	return nil
}
