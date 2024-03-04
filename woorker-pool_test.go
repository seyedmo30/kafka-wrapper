package kafkawrapper

import (
	"context"
	"fmt"
	"math/rand"
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
	w := newWorker(1, 2, mockFirstClassFunc, workQueue, resultQueue, errorChannel)

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

func mockFirstClassFunc(ctx context.Context, workQueue chan ReadMessageDTO, resultQueue chan WriteMessageDTO, errorChannel chan error, done chan struct{}) {
	read := <-workQueue
	panic("Something went wrong!")
	if rand.Intn(10) == 5 {
		panic("Something went wrong!")

	}
	// Mock implementation for writing to resultQueue
	res := generateWriteMessage(string(read.Value))
	resultQueue <- res

	done <- struct{}{}

}

func generateWriteMessage(pre string) WriteMessageDTO {
	time.Sleep(time.Second * time.Duration(rand.Intn(10)+1))

	return WriteMessageDTO{

		Value: []byte(pre + "___" + generateRandomString(2)),
	}
}

func generateMockReadMessage() ReadMessageDTO {
	time.Sleep(time.Second * time.Duration(rand.Intn(10)+1))

	return ReadMessageDTO{

		Value: []byte(generateRandomString(2)),
	}
}

func generateRandomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	seededRand := rand.New(rand.NewSource(time.Now().UnixNano()))
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[seededRand.Intn(len(charset))]
	}

	return string(b)
}
