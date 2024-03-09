package kafkawrapper

import (
	"context"
	"strings"
	"time"
)

func Run(ctx context.Context, kafkaConsumer kafkaConsumer, method FirstClassFunc, kafkaPubliosher kafkaPublisher, optionalConfiguration ...OptionalConfiguration) chan error {
	opt := validateOptionalConfiguration(optionalConfiguration...)
	errCh := make(chan error, 5)
	go func() {
		var err error
		readMessageDTOCh := make(chan ReadMessageDTO, 3)
		writeMessageDTOCh := make(chan WriteMessageDTO, 3)
		for {

			err = kafkaConsumer.consumerConnection()

			if err != nil {
				// TODO
				logger.Error("kafka Consumer cant connect", "external_error", err.Error())

				errCh <- err
				time.Sleep(5 * time.Second)
			} else {
				break
			}
		}
		logger.Info("kafkaConsumer success : " + kafkaConsumer.topic)

		for {

			err = kafkaPubliosher.publisherConnection()

			if err != nil {
				// TODO
				logger.Error("kafka publisher cant connect", "external_error", err.Error())

				errCh <- err
				time.Sleep(5 * time.Second)
			} else {
				break
			}
		}

		logger.Info("kafkaPubliosher success : " + kafkaPubliosher.topic)
		// consumer
		go func() {
			defer close(readMessageDTOCh)

			for {
				logger.Debug("start read msg", "topic", kafkaConsumer.topic)

				msg, err := kafkaConsumer.getter(ctx)

				if err != nil {
					logger.Error("kafka cant read message ", "external_error", err.Error(), "topic", kafkaConsumer.topic)
					errCh <- err

					if strings.Contains(err.Error(), "connection refused") {
						for {

							errCh <- err

							time.Sleep(5 * time.Second)

							if err = kafkaConsumer.consumerConnection(); err == nil {
								break
							}
							logger.Error("kafka Consumer cant connect", "external_error", err.Error())
						}

					}
					continue
				}
				headers := make([]Header, 0, 1)
				for _, header := range msg.Headers {
					headers = append(headers, Header{Key: header.Key})

				}
				readMessageDTOCh <- ReadMessageDTO{Key: msg.Key, Value: msg.Value, Headers: headers}
				logger.Debug("msg send to chan ReadMessageDTO success :", "value", string(msg.Value))
			}
		}()

		// publisher
		go func() {
			defer close(writeMessageDTOCh)

			for message := range writeMessageDTOCh {
				if err := kafkaPubliosher.setter(ctx, message); err != nil {
					logger.Error("kafka Consumer cant connect", "external_error", err.Error())
					errCh <- err

					if strings.Contains(err.Error(), "connection refused") {
						for {

							errCh <- err

							time.Sleep(5 * time.Second)

							if err = kafkaPubliosher.publisherConnection(); err == nil {
								break
							}
							logger.Error("kafka Consumer cant connect", "external_error", err.Error())
						}

					}
				}
			}

		}()

		// worker pool
		var i uint8

		for i = 0; i < opt.Worker; i++ {
			nameWorker := kafkaConsumer.topic + "_to_" + kafkaPubliosher.topic
			w := newWorker(i+1, nameWorker, method, readMessageDTOCh, writeMessageDTOCh, errCh, opt)
			go w.start(ctx)

		}
	}()
	return errCh
}
