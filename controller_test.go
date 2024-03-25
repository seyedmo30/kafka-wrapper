package kafkawrapper

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"testing"
	"time"
)

func TestRun(t *testing.T) {
	socket := "10.21.15.99:21692"

	opt := OptionalConfiguration{
		Worker:             5,
		Retry:              3,
		Timeout:            10,
		NumberFuncInWorker: 5,
	}
	// Create a context with a cancel function
	ctx, cancel := context.WithCancel(context.Background())

	// Create a channel to receive signals
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	// Goroutine to listen for SIGINT or SIGTERM
	go func() {
		<-sigs
		logger.Warn("Received SIGINT or SIGTERM")
		cancel() // Call cancel function to close the context
		time.Sleep(time.Second * 10)
		os.Exit(0)
	}()

	topic_publisher0 := "_test1"
	publisher0 := KafkaPublisherSetup(socket, topic_publisher0)

	go func() {
		errCh := RunOnlyPublisher(ctx, publisher0, mockPublisher())

		for value := range errCh {
			logger.Error(" Received error:" + value.Error())
		}
	}()
	topic_consumer1 := "_test1"
	topic_publisher1 := "_test2"
	groupID_consumer1 := "groupid_1"

	consumer1 := KafkaConsumerSetup(socket, topic_consumer1, groupID_consumer1)
	publisher1 := KafkaPublisherSetup(socket, topic_publisher1)
	go func() {

		errCh := Run(ctx, consumer1, mockFirstClassFunc, publisher1, opt)

		for value := range errCh {
			logger.Error(" Received error:" + value.Error())
		}
	}()
	topic_consumer2 := "_test2"
	topic_publisher2 := "_test3"
	groupID_consumer2 := "groupid_2"
	consumer2 := KafkaConsumerSetup(socket, topic_consumer2, groupID_consumer2)
	publisher2 := KafkaPublisherSetup(socket, topic_publisher2)
	go func() {

		errCh := Run(ctx, consumer2, mockFirstClassFunc, publisher2, opt)

		for value := range errCh {
			logger.Error(" Received error:" + value.Error())
		}
	}()
	topic_consumer3 := "_test3"
	groupID_consumer3 := "groupid_3"
	consumer3 := KafkaConsumerSetup(socket, topic_consumer3, groupID_consumer3)

	errCh := RunOnlyConsumer(ctx, consumer3, mockFirstClassFuncOnlyConsumer, opt)

	for value := range errCh {
		logger.Error(" Received error:" + value.Error())
	}
}
