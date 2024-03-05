package kafkawrapper

import (
	"context"
	"fmt"
	"testing"
)

func TestRun(t *testing.T) {
	var errCh chan error
	socket := "localhost:9092"
	topic_consumer := "test1"
	topic_publisher := "test2"
	groupID_consumer := "fre"
	consumer := KafkaConsumerSetup(socket, topic_consumer, groupID_consumer)
	publisher := KafkaPublisherSetup(socket, topic_publisher)
	fmt.Println("start run")
	errCh = Run(context.Background(), consumer, mockFirstClassFunc, publisher)

	topic_consumer2 := "test2"
	topic_publisher2 := "test3"
	groupID_consumer2 := "fggf"
	consumer2 := KafkaConsumerSetup(socket, topic_consumer2, groupID_consumer2)
	publisher2 := KafkaPublisherSetup(socket, topic_publisher2)
	errCh = Run(context.Background(), consumer2, mockFirstClassFunc, publisher2)

	for {
		err := <-errCh
		fmt.Println(err)
	}
}
