package kafkawrapper

import (
	"context"
	"fmt"
	"testing"
)

func TestRun(t *testing.T) {
	socket := "10.21.15.99:21692"
	topic_consumer := "_test1"
	topic_publisher := "_test2"
	groupID_consumer := "efrew"
	consumer := KafkaConsumerSetup(socket, topic_consumer, groupID_consumer)
	publisher := KafkaPublisherSetup(socket, topic_publisher)
	errCh := Run(context.Background(), consumer, mockFirstClassFunc, publisher)

	topic_consumer2 := "_test2"
	topic_publisher2 := "_test3"
	groupID_consumer2 := "fggf"
	consumer2 := KafkaConsumerSetup(socket, topic_consumer2, groupID_consumer2)
	publisher2 := KafkaPublisherSetup(socket, topic_publisher2)
	errCh = Run(context.Background(), consumer2, mockFirstClassFunc, publisher2)

	for {
		err := <-errCh
		fmt.Println(err)
	}
}
