package kafkawrapper

import (
	"context"
	"strings"
)

type FirstClassFunc func(ctx context.Context, readMessage ReadMessageDTO) (WriteMessageDTO, error)

func Run(ctx context.Context, kafkaConsumer kafkaConsumer, method FirstClassFunc, kafkaPubliosher kafkaPublisher) {

	err := kafkaConsumer.consumerConnection()

	if err != nil {
		// TODO
		logs().Error("kafka cant coonect : " + err.Error())

	}

	go func() {
		for {

			msg, err := kafkaConsumer.getter().KafkaReader.ReadMessage(ctx)
			if err != nil {
				logs().Error("kafka cant read message : " + err.Error())
				if strings.Contains(err.Error(), "network is unreachable") {
					//  TODO
					_ = kafkaConsumer.consumerReconnection()

				}
				logs().Error("kafka cant read message : " + err.Error())

				continue
			}

			// TODO
			// worker pool

			readMessage := ReadMessageDTO{
				Key:   msg.Key,
				Value: msg.Value,
			}
			writeMessage, err := method(ctx, readMessage)

		}

	}()

	select {}
}
