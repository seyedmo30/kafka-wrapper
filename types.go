package kafkawrapper

import "context"

type Header struct {
	Key   string
	Value []byte
}

type ReadMessageDTO struct {
	Key     []byte
	Value   []byte
	Headers []Header
	Retry   uint8
}

type WriteMessageDTO struct {
	Key     []byte
	Value   []byte
	Headers []Header
}

type ResponseDTO struct {
	isSuccess      bool
	readMessageDTO ReadMessageDTO
}

type OptionalConfiguration struct {
	Worker                     uint8
	Retry                      uint8
	Timeout                    uint8
	NumberFuncInWorker         uint8
	ErrorChannelBufferSize     uint8
	ConsumerChannelBufferSize  uint8
	PublisherChannelBufferSize uint8
}

type FirstClassFunc func(ctx context.Context, readMessageCh chan ReadMessageDTO, writeMessageCh chan WriteMessageDTO, errCh chan error, response chan ResponseDTO)

type FirstClassFuncOnlyConsumer func(ctx context.Context, readMessageCh chan ReadMessageDTO, errCh chan error, response chan ResponseDTO)

type FirstClassFuncOnlyPublisher func(ctx context.Context, writeMessageCh chan WriteMessageDTO, errCh chan error, response chan ResponseDTO)
