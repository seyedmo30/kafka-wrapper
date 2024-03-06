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
}

type WriteMessageDTO struct {
	Key     []byte
	Value   []byte
	Headers []Header
}

type OptionalConfiguration struct {
	Worker             int
	Retry              int
	Timeout            int
	NumberFuncInWorker int
}

type FirstClassFunc func(ctx context.Context, readMessageCh chan ReadMessageDTO, writeMessageCh chan WriteMessageDTO, errCh chan error, done chan struct{})
