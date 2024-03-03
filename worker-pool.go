package kafkawrapper

import (
	"context"
	"errors"
	"time"
)

type worker struct {
	id           int
	function     FirstClassFunc
	workQueue    chan ReadMessageDTO
	resultQueue  chan WriteMessageDTO
	errorChannel chan error
}

func newWorker(id int, fn FirstClassFunc, workQueue chan ReadMessageDTO, resultQueue chan WriteMessageDTO, errorChannel chan error) *worker {
	return &worker{
		id:           id,
		function:     fn,
		workQueue:    workQueue,
		resultQueue:  resultQueue,
		errorChannel: errorChannel,
	}
}
func (w *worker) start(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:

			ctx, cancel := context.WithTimeout(ctx, 50*time.Second) // Set your desired timeout duration
			defer cancel()
			done := make(chan struct{}, 1)
			finish := make(chan struct{}, 1)

			go func(t chan struct{}) {

				select {
				case <-done:
					finish <- struct{}{}

				case <-time.After(time.Second * 50):
					w.errorChannel <- errors.New("timeout")
					finish <- struct{}{}
				}

				t <- struct{}{}

			}(done)

			go w.function(ctx, w.workQueue, w.resultQueue, w.errorChannel, done)

			<-finish

			cancel()
			close(done)
			close(finish)

		}
	}
}
