package kafkawrapper

import (
	"context"
	"fmt"
	"strings"
)

func Run(ctx context.Context, kafkaConsumer kafkaConsumer, method FirstClassFunc, kafkaPubliosher kafkaPublisher, optionalConfiguration ...OptionalConfiguration) {

	opt := validateOptionalConfiguration(optionalConfiguration...)
	readMessageDTOCh := make(chan ReadMessageDTO, 50)
	writeMessageDTOCh := make(chan WriteMessageDTO, 50)
	errorChannel := make(chan error, 50)
	err := kafkaConsumer.consumerConnection()

	if err != nil {
		// TODO
		logs().Error("kafka cunsumer cant coonect : " + err.Error())
		panic("please ckeck cunsumer connection ")
	}

	err = kafkaPubliosher.publisherConnection()

	if err != nil {
		// TODO
		logs().Error("kafka publisher cant coonect : " + err.Error())
		panic("please ckeck publisher connection ")
	}

	// consumer
	go func() {
		defer close(readMessageDTOCh)

		for {
			fmt.Println("start read msg")
			msg, err := kafkaConsumer.getter().KafkaReader.ReadMessage(ctx)
			fmt.Println(string(msg.Value))

			if err != nil {
				logs().Error("kafka cant read message : " + err.Error())
				if strings.Contains(err.Error(), "network is unreachable") {
					logs().Error("network is unreachable : " + err.Error())
					//  TODO
					_ = kafkaConsumer.consumerReconnection()

				}
				continue
			}
			headers := make([]Header, 0, 1)
			for _, header := range msg.Headers {
				headers = append(headers, Header{Key: header.Key})

			}
			readMessageDTOCh <- ReadMessageDTO{Key: msg.Key, Value: msg.Value, Headers: headers}

		}
	}()

	// publisher
	go func() {
		defer close(writeMessageDTOCh)

		for message := range writeMessageDTOCh {
			if err := kafkaPubliosher.setter(ctx, message); err != nil {
				logs().Warn(err.Error())

			}
		}

	}()

	// worker pool
	for i := 0; i < opt.Worker; i++ {

		w := newWorker(i+1, 10, method, readMessageDTOCh, writeMessageDTOCh, errorChannel)
		go w.start(ctx)

	}

	select {}
}
