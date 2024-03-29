package kafkawrapper

import (
	"context"
	"fmt"
	"testing"
	"time"

	"os"

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
	socket := "10.21.15.99:21692"
	topic := "_test1"
	groupID := "fsdsdewf"

	consumer := KafkaConsumerSetup(socket, topic, groupID)

	err := consumer.consumerConnection(OptionalConfiguration{})

	assert.NoError(t, err, "Expected no error during Kafka connection")
	for i := 1; ; i++ {

		if msg, err := consumer.getter(context.Background()); err != nil {

			fmt.Fprintln(os.Stdout, []any{"err :", i, err}...)
		} else {

			fmt.Printf("%d _ %+v \n", i, string(msg.Value))
		}

	}

}

func TestConsumerConnection_Failure(t *testing.T) {
	socket := "invalid_host:9092"
	topic := "test"
	groupID := "test_group3"

	consumer := KafkaConsumerSetup(socket, topic, groupID)

	err := consumer.consumerConnection(OptionalConfiguration{})

	assert.Error(t, err, "Expected ervror during Kafka connection")

}

func TestPublisherConnection_Success(t *testing.T) {
	socket := "localhost:9092"
	topic := "_test1"
	publisher := KafkaPublisherSetup(socket, topic)

	err := publisher.publisherConnection(OptionalConfiguration{})

	assert.NoError(t, err, "Expected no error during Kafka connection")

	for i := 0; i < 20; i++ {

		err = publisher.setter(context.Background(), WriteMessageDTO{
			Key:   []byte(generateRandomString(3)),
			Value: []byte(generateRandomString(3)),
		})
		fmt.Println(err)
		// time.Sleep(1 * time.Second)
	}
}

func TestKafkaIntegration(t *testing.T) {
	// Setup Kafka broker configuration
	socket := "localhost:9092"
	topic := "test1"
	groupID := "1"

	// Setup Kafka consumer
	consumer := KafkaConsumerSetup(socket, topic, groupID)
	defer consumer.close()

	// Setup Kafka publisher
	publisher := KafkaPublisherSetup(socket, topic)
	defer publisher.close()

	// Connect Kafka consumer
	if err := consumer.consumerConnection(OptionalConfiguration{}); err != nil {
		t.Fatalf("Failed to connect consumer: %v", err)
	}

	// Connect Kafka publisher
	if err := publisher.publisherConnection(OptionalConfiguration{}); err != nil {
		t.Fatalf("Failed to connect publisher: %v", err)
	}

	// Define test message
	message := WriteMessageDTO{
		Key:   []byte("test_key"),
		Value: []byte("test_value"),
	}

	// Publish test message
	ctx := context.Background()
	if err := publisher.setter(ctx, message); err != nil {
		t.Fatalf("Failed to publish message: %v", err)
	}

	// Consume test message
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	msg, err := consumer.getter(ctx)
	if err != nil {
		t.Fatalf("Failed to consume message: %v", err)
	}

	// Verify consumed message
	expectedMessage := ReadMessageDTO{
		Key:   message.Key,
		Value: message.Value,
	}
	if !bytesEqual(msg.Key, expectedMessage.Key) || !bytesEqual(msg.Value, expectedMessage.Value) {
		t.Errorf("Expected message %+v, got %+v", expectedMessage, msg)
	}
}
func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
