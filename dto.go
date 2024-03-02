package kafkawrapper

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
