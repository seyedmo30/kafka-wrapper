package kafkawrapper

import (
	"testing"

	"github.com/stretchr/testify/assert"
	// Update this with your actual package import path
)

func TestKafkaConsumerSetup(t *testing.T) {
	// Define test parameters
	socket := "localhost:9092"
	topic := "test"
	groupID := "test_group3"

	// Call the function being tested
	consumer := KafkaConsumerSetup(socket, topic, groupID)

	// Assertions
	assert.Equal(t, socket, consumer.socket, "Socket should match")
	assert.Equal(t, topic, consumer.topic, "Topic should match")
	assert.Equal(t, groupID, consumer.groupID, "GroupID should match")
}

func TestConsumerConnection_Success(t *testing.T) {
	// Define test parameters
	socket := "localhost:9092"
	topic := "test"
	groupID := "test_group3"

	// Create a kafkaConsumer
	consumer := KafkaConsumerSetup(socket, topic, groupID)

	// Call the method being tested
	kafkaReader, err := consumer.consumerConnection()

	// Assertions
	assert.NoError(t, err, "Expected no error during Kafka connection")
	assert.NotNil(t, kafkaReader.KafkaReader, "Kafka reader should not be nil")
}

func TestConsumerConnection_Failure(t *testing.T) {
	// Define test parameters
	socket := "invalid_host:9092"
	topic := "test"
	groupID := "test_group3"

	// Create a kafkaConsumer
	consumer := KafkaConsumerSetup(socket, topic, groupID)

	// Call the method being tested
	kafkaReader, err := consumer.consumerConnection()

	// Assertions
	assert.Error(t, err, "Expected error during Kafka connection")
	assert.Nil(t, kafkaReader.KafkaReader, "Kafka reader should be nil")
}
