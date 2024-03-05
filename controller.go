package kafkawrapper

import (
	"context"
	"fmt"
	"strings"
)

func Run(ctx context.Context, kafkaConsumer kafkaConsumer, method FirstClassFunc, kafkaPubliosher kafkaPublisher, optionalConfiguration ...OptionalConfiguration) chan error {
	errCh := make(chan error, 3)
	go func() {

		opt := validateOptionalConfiguration(optionalConfiguration...)
		readMessageDTOCh := make(chan ReadMessageDTO, 3)
		writeMessageDTOCh := make(chan WriteMessageDTO, 3)
		errorChannel := make(chan error, 3)
		err := kafkaConsumer.consumerConnection()

		if err != nil {
			// TODO
			logs().Error("kafka cunsumer cant coonect : " + err.Error())
			errCh <- err
		}
		logs().Info("kafkaConsumer success : " + kafkaConsumer.topic)

		err = kafkaPubliosher.publisherConnection()

		if err != nil {
			// TODO
			logs().Error("kafka publisher cant coonect : " + err.Error())
			errCh <- err

		}

		logs().Info("kafkaPubliosher success : " + kafkaPubliosher.topic)
		// consumer
		go func() {
			defer close(readMessageDTOCh)

			for {
				fmt.Println("start read msg")
				msg, err := kafkaConsumer.getter(ctx)
				fmt.Println(string(msg.Value))

				if err != nil {
					logs().Error("kafka cant read message : " + err.Error())
					errCh <- err

					if strings.Contains(err.Error(), "network is unreachable") {
						logs().Error("network is unreachable : " + err.Error())
						//  TODO
						errCh <- err

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
					errCh <- err

				}
			}

		}()

		// worker pool
		for i := 0; i < opt.Worker; i++ {

			w := newWorker(i+1, 10, method, readMessageDTOCh, writeMessageDTOCh, errorChannel)
			go w.start(ctx)

		}
	}()
	return errCh
}
