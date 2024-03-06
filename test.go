package kafkawrapper

import (
	"context"
	"math/rand"
	"time"
)

func mockFirstClassFunc(ctx context.Context, workQueue chan ReadMessageDTO, resultQueue chan WriteMessageDTO, errorChannel chan error, done chan struct{}) {
	read := <-workQueue
	// if rand.Intn(10) == 5 {
	// 	panic("Something went wrong!")

	// }
	// Mock implementation for writing to resultQueue
	res := generateWriteMessage(string(read.Value))
	resultQueue <- res

	done <- struct{}{}

}

func generateWriteMessage(pre string) WriteMessageDTO {
	time.Sleep(time.Second * time.Duration(rand.Intn(2)+1))

	return WriteMessageDTO{

		Value: []byte(pre + "___" + generateRandomString(2)),
	}
}

func generateMockReadMessage() ReadMessageDTO {
	time.Sleep(time.Second * time.Duration(rand.Intn(2)+1))

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
