package kafkawrapper

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestWorkerStart(t *testing.T) {
	// Create channels for testing
	workQueue := make(chan ReadMessageDTO, 5)
	resultQueue := make(chan WriteMessageDTO, 5)
	errorChannel := make(chan error, 5)
	go func() {
		for {
			read := generateMockReadMessage()
			fmt.Println("$$$ read :" + string(read.Value))

			workQueue <- read

		}
	}()

	// Create a new worker
	w := newWorker(1, "test_topic",2, mockFirstClassFunc, workQueue, resultQueue, errorChannel, OptionalConfiguration{Worker: 1, Retry: 1, Timeout: 5, NumberFuncInWorker: 10})

	// Start the worker
	go w.start(context.Background())

	// Send a mock message to the workQueue

	// Check if resultQueue received any message
	for {

		select {
		case result := <-resultQueue:
			fmt.Println("&&& result : ", string(result.Value))
		case err := <-errorChannel:
			fmt.Println("err : ", err)

			// Test passes if resultQueue received a message
		case <-time.After(10 * time.Second):
			t.Errorf("worker did not process the message within expected time")
		}
	}
}
