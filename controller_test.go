package kafkawrapper

import (
	"context"
	"fmt"
	"testing"
)

func TestRun(t *testing.T) {
	socket := "localhost:9092"
	topic_consumer := "test"
	topic_publisher := "test3"
	groupID_consumer := "test_1"
	consumer := KafkaConsumerSetup(socket, topic_consumer, groupID_consumer)
	publisher := KafkaPublisherSetup(socket, topic_publisher)
	fmt.Println("start run")
	Run(context.Background(), consumer, mockFirstClassFunc, publisher)

}
