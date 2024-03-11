package kafkawrapper

import (
	"context"
	"math/rand"
	"time"
)

func mockFirstClassFunc(ctx context.Context, workQueue chan ReadMessageDTO, resultQueue chan WriteMessageDTO, errorChannel chan error, response chan ResponseDTO) {
	read := <-workQueue
	if rand.Intn(5) == 1 {
		panic("Something went wrong!")

	}
	if rand.Intn(3) == 1 {
		response <- ResponseDTO{isSuccess: false, readMessageDTO: read}

	} else {

		// Mock implementation for writing to resultQueue
		res := generateWriteMessage(string(read.Value))
		resultQueue <- res
		response <- ResponseDTO{isSuccess: true}
	}

}

func mockFirstClassFuncOnlyConsumer(ctx context.Context, workQueue chan ReadMessageDTO, errorChannel chan error, response chan ResponseDTO) {
	read := <-workQueue
	if rand.Intn(5) == 1 {
		panic("Something went wrong!")

	}
	if rand.Intn(5) == 1 {
		response <- ResponseDTO{isSuccess: false, readMessageDTO: read}

	} else {

		time.Sleep(time.Second * time.Duration(rand.Intn(2)+1))

		logger.Info("success :) mockFirstClassFuncOnlyConsumer")
		response <- ResponseDTO{isSuccess: true}
	}

}

func generateWriteMessage(pre string) WriteMessageDTO {
	time.Sleep(time.Second * time.Duration(rand.Intn(2)+1))

	return WriteMessageDTO{

		Key:   []byte(pre + "___" + generateRandomString(2)),
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
