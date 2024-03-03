package kafkawrapper

import (
	"context"
	"strings"
)

func Run(ctx context.Context, kafkaConsumer kafkaConsumer, method FirstClassFunc, kafkaPubliosher kafkaPublisher, optionalConfiguration ...OptionalConfiguration) {

	opt := validateOptionalConfiguration(optionalConfiguration...)
	readMessageDTOCh := make(chan ReadMessageDTO, 50)
	err := kafkaConsumer.consumerConnection()

	if err != nil {
		// TODO
		logs().Error("kafka cant coonect : " + err.Error())

	}

	go func() {
		defer close(readMessageDTOCh)

		for {

			msg, err := kafkaConsumer.getter().KafkaReader.ReadMessage(ctx)
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

	// TODO
	// worker pool

	// readMessage := ReadMessageDTO{
	// 	Key:   msg.Key,
	// 	Value: msg.Value,
	// }
	// writeMessage, err := method(ctx, readMessage)

	for i := 0; i < opt.Worker; i++ {
		go func(workerID int) {

		}(i + 1)

	}

	select {}
}
