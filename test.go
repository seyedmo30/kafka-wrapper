package kafkawrapper

import (
	"context"
	"math/rand"
	"time"

	"github.com/seyedmo30/kafka-wrapper/pkg"
)

func mockFirstClassFunc(ctx context.Context, workQueue chan ReadMessageDTO, resultQueue chan WriteMessageDTO, errorChannel chan error, response chan ResponseDTO) {

	read := <-workQueue

	logReq := pkg.StdLog{
		Type:      "HTTP Request",
		Message:   "Received a GET request",
		ReqMethod: "GET",
		ReqURL:    "/api/v1/users",
		ReqParams: "",
		ReqQuery:  "page=1&limit=10",
		ReqHeaders: map[string]string{
			"User-Agent": "Mozilla/5.0",
			"Accept":     "application/json",
		},
	}

	logReq.Log("get req test")

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

	log := pkg.StdLog{
		Error:            nil,
		ErrorType:        "Validation Error",
		ErrorDescription: "Invalid input data",
		ErrorStatus:      400,
		UserID:           "12345",
		Username:         "mostafa",
		Type:             "HTTP Request",
		Payload:          "{\"key\":\"value\"}",
		Message:          "Invalid input data received",
		ReqMethod:        "POST",
		ReqURL:           "/api/v1/user",
		ReqParams:        "",
		ReqQuery:         "",
		ResStatus:        400,
		ResStatusText:    "Bad Request",
		Process:          nil,
	}
	log.Log("test log")

}

func mockPublisher() chan WriteMessageDTO {
	writeMessageCh := make(chan WriteMessageDTO, 5)
	go func() {

		for {

			gen := generateWriteMessage(generateRandomString(2))
			writeMessageCh <- gen
			logger.Debug("send success to topic", "msg", string(gen.Key))
		}
	}()
	return writeMessageCh
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
