package kafkawrapper

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestKafkaConsumerSetup(t *testing.T) {
	socket := "localhost:9092"
	topic := "test"
	groupID := "test_group3"

	consumer := KafkaConsumerSetup(socket, topic, groupID)

	assert.Equal(t, socket, consumer.socket, "Socket should match")
	assert.Equal(t, topic, consumer.topic, "Topic should match")
	assert.Equal(t, groupID, consumer.groupID, "GroupID should match")
}

func TestConsumerConnection_Success(t *testing.T) {
	socket := "localhost:9092"
	topic := "test"
	groupID := "test_1"

	consumer := KafkaConsumerSetup(socket, topic, groupID)

	err := consumer.consumerConnection()

	assert.NoError(t, err, "Expected no error during Kafka connection")

	msg, err := consumer.getter(context.Background())
	fmt.Printf("%+v \n", string(msg.Value))
	fmt.Println("err :", err)
	// consumer.close()
	// assert.NoError(t, err, "Expected no error during Kafka connection")

	msg, err = consumer.getter(context.Background())
	fmt.Printf("%+v \n", string(msg.Value))
	fmt.Println("err :", err)

	msg, err = consumer.getter(context.Background())
	fmt.Printf("%+v \n", string(msg.Value))
	fmt.Println("err :", err)

	msg, err = consumer.getter(context.Background())
	fmt.Printf("%+v \n", string(msg.Value))
	fmt.Println("err :", err)

	msg, err = consumer.getter(context.Background())
	fmt.Printf("%+v \n", string(msg.Value))
	fmt.Println("err :", err)

	
}

func TestConsumerConnection_Failure(t *testing.T) {
	socket := "invalid_host:9092"
	topic := "test"
	groupID := "test_group3"

	consumer := KafkaConsumerSetup(socket, topic, groupID)

	err := consumer.consumerConnection()

	assert.Error(t, err, "Expected ervror during Kafka connection")

}

func TestPublisherConnection_Success(t *testing.T) {
	socket := "localhost:9092"
	topic := "test"
	publisher := KafkaPublisherSetup(socket, topic)

	err := publisher.publisherConnection()

	assert.NoError(t, err, "Expected no error during Kafka connection")

	publisher.close()

	err = publisher.setter(context.Background(), WriteMessageDTO{
		Key:   []byte("1"),
		Value: []byte("Hello, Kafka!"),
	})
	fmt.Println("err", err)

	err = publisher.publisherConnection()
	fmt.Println("err", err)

	err = publisher.publisherConnection()

	fmt.Println("err", err)
	err = publisher.publisherConnection()
	fmt.Println("err", err)
	err = publisher.publisherConnection()
	fmt.Println("err", err)
	err = publisher.publisherConnection()
	fmt.Println("err", err)
	err = publisher.publisherConnection()
	fmt.Println("err", err)
	err = publisher.publisherConnection()

	go publisher.setter(context.Background(), WriteMessageDTO{
		Key:   []byte("2"),
		Value: []byte("Hello, Kafka!"),
	})
	go publisher.setter(context.Background(), WriteMessageDTO{
		Key:   []byte("3"),
		Value: []byte("Hello, Kafka!"),
	})
	go publisher.setter(context.Background(), WriteMessageDTO{
		Key:   []byte("4"),
		Value: []byte("Hello, Kafka!"),
	})
	go publisher.setter(context.Background(), WriteMessageDTO{
		Key:   []byte("5"),
		Value: []byte("Hello, Kafka!"),
	})
	go publisher.setter(context.Background(), WriteMessageDTO{
		Key:   []byte("6"),
		Value: []byte("Hello, Kafka!"),
	})
	time.Sleep(time.Second * 5)

}
