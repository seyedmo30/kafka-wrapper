package kafkawrapper

import (
	"context"
	"strings"
	"time"
)

func Run(ctx context.Context, kafkaConsumer kafkaConsumer, method FirstClassFunc, kafkaPublisher kafkaPublisher, optionalConfiguration ...OptionalConfiguration) chan error {
	opt := validateOptionalConfiguration(optionalConfiguration...)
	errCh := make(chan error, opt.ErrorChannelBufferSize)
	go func() {

		readMessageDTOCh := consumerController(ctx, kafkaConsumer, errCh, opt)

		writeMessageDTOCh := publisherController(ctx, kafkaPublisher, errCh, opt)

		// worker pool
		var i uint8

		for i = 0; i < opt.Worker; i++ {
			nameWorker := kafkaConsumer.topic + "_to_" + kafkaPublisher.topic
			w := newWorker(i+1, nameWorker, method, readMessageDTOCh, writeMessageDTOCh, errCh, opt)
			go w.start(ctx)

		}
	}()
	return errCh
}

func RunOnlyPublisher(ctx context.Context, kafkaPublisher kafkaPublisher, writeMessageCh chan WriteMessageDTO, optionalConfiguration ...OptionalConfiguration) chan error {
	opt := validateOptionalConfiguration(optionalConfiguration...)

	errCh := make(chan error, opt.ErrorChannelBufferSize)

	writeMessageDTOCh := publisherController(ctx, kafkaPublisher, errCh, opt)
	go func() {

		for writeMessage := range writeMessageCh {

			writeMessageDTOCh <- writeMessage
		}
		// worker pool
	}()
	return errCh
}

func RunOnlyConsumer(ctx context.Context, kafkaConsumer kafkaConsumer, method FirstClassFuncOnlyConsumer, optionalConfiguration ...OptionalConfiguration) chan error {
	opt := validateOptionalConfiguration(optionalConfiguration...)
	errCh := make(chan error, opt.ErrorChannelBufferSize)
	go func() {

		readMessageDTOCh := consumerController(ctx, kafkaConsumer, errCh, opt)

		// worker pool
		var i uint8

		for i = 0; i < opt.Worker; i++ {
			nameWorker := "only_consumer_" + kafkaConsumer.topic
			w := newWorkerOnlyConsumer(i+1, nameWorker, method, readMessageDTOCh, errCh, opt)
			go w.start(ctx)

		}
	}()
	return errCh
}

func consumerController(ctx context.Context, kafkaConsumer kafkaConsumer, errCh chan error, opt OptionalConfiguration) chan ReadMessageDTO {

	readMessageDTOCh := make(chan ReadMessageDTO, opt.ConsumerChannelBufferSize)
	for {

		err := kafkaConsumer.consumerConnection(opt)

		if err != nil {
			logger.Error("kafka Consumer cant connect", "external_error", err.Error())

			errCh <- err
			time.Sleep(5 * time.Second)
		} else {
			break
		}
	}
	logger.Info("kafkaConsumer success : " + kafkaConsumer.topic)

	// consumer
	go func() {
		defer close(readMessageDTOCh)

		for {

			select {
			case <-ctx.Done():
				if err := kafkaConsumer.close(); err != nil {
					logger.Debug(err.Error())

				}
				select {}
			default:
				logger.Debug("start read msg", "topic", kafkaConsumer.topic)

				msg, err := kafkaConsumer.getter(ctx)

				if err != nil {
					logger.Error("kafka cant read message ", "external_error", err.Error(), "topic", kafkaConsumer.topic)
					errCh <- err

					if strings.Contains(err.Error(), "connection refused") {
						for {

							errCh <- err

							time.Sleep(5 * time.Second)

							if err = kafkaConsumer.consumerConnection(opt); err == nil {
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
		}
	}()
	return readMessageDTOCh
}

func publisherController(ctx context.Context, kafkaPublisher kafkaPublisher, errCh chan error, opt OptionalConfiguration) chan WriteMessageDTO {
	writeMessageDTOCh := make(chan WriteMessageDTO, opt.PublisherChannelBufferSize)

	for {

		err := kafkaPublisher.publisherConnection(opt)

		if err != nil {
			// TODO
			logger.Error("kafka publisher cant connect", "external_error", err.Error())

			errCh <- err
			time.Sleep(5 * time.Second)
		} else {
			break
		}
	}

	logger.Info("kafkaPublisher success : " + kafkaPublisher.topic)

	// publisher
	go func() {
		defer close(writeMessageDTOCh)

		for message := range writeMessageDTOCh {
			if err := kafkaPublisher.setter(context.Background(), message); err != nil {
				logger.Error("kafka Consumer cant connect", "external_error", err.Error())
				errCh <- err

				if strings.Contains(err.Error(), "connection refused") {
					for {

						errCh <- err

						time.Sleep(5 * time.Second)

						if err = kafkaPublisher.publisherConnection(opt); err == nil {
							break
						}
						logger.Error("kafka Consumer cant connect", "external_error", err.Error())
					}

				}
			}
		}

	}()

	return writeMessageDTOCh
}
