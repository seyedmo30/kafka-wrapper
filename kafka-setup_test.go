package kafkawrapper

import (
	"context"
	"fmt"
	"testing"

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
	groupID := "test_group6"

	consumer := KafkaConsumerSetup(socket, topic, groupID)

	err := consumer.consumerConnection()

	assert.NoError(t, err, "Expected no error during Kafka connection")

	msg, err := consumer.getter().KafkaReader.ReadMessage(context.Background())

	fmt.Println(msg, err)

}

func TestConsumerConnection_Failure(t *testing.T) {
	socket := "invalid_host:9092"
	topic := "test"
	groupID := "test_group3"

	consumer := KafkaConsumerSetup(socket, topic, groupID)

	err := consumer.consumerConnection()

	assert.Error(t, err, "Expected ervror during Kafka connection")

}

func TestPublisherConnection_Failure(t *testing.T) {
	socket := "localhost:9092"
	topic := "test"
	publisher := KafkaPublisherSetup(socket, topic)

	err := publisher.publisherConnection()

	assert.NoError(t, err, "Expected no error during Kafka connection")

}
