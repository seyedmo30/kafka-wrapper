package kafkawrapper

import (
	"context"
	"errors"
	"time"
)

type worker struct {
	id               int
	limitRunFunction int
	function         FirstClassFunc
	workQueue        chan ReadMessageDTO
	resultQueue      chan WriteMessageDTO
	errorChannel     chan error
}

func newWorker(id int, limitRunFunction int, fn FirstClassFunc, workQueue chan ReadMessageDTO, resultQueue chan WriteMessageDTO, errorChannel chan error) *worker {
	return &worker{
		id:               id,
		limitRunFunction: limitRunFunction,
		function:         fn,
		workQueue:        workQueue,
		resultQueue:      resultQueue,
		errorChannel:     errorChannel,
	}
}
func (w *worker) start(ctx context.Context) {

	concurrentRunFunction := make(chan struct{}, w.limitRunFunction)
	for {
		select {
		case <-ctx.Done():
			return
		default:

			ctx, cancel := context.WithTimeout(ctx, 50*time.Second) // Set your desired timeout duration
			defer cancel()
			done := make(chan struct{}, 1)
			finish := make(chan struct{}, 1)

			go func(t chan struct{}, concurrentRunFunction chan struct{}) {

				select {
				case <-done:
					concurrentRunFunction <- struct{}{}

				case <-time.After(time.Second * 50):
					w.errorChannel <- errors.New("timeout")

					concurrentRunFunction <- struct{}{}

				}

				t <- struct{}{}

			}(done, concurrentRunFunction)

			go w.function(ctx, w.workQueue, w.resultQueue, w.errorChannel, done)

			<-concurrentRunFunction

			cancel()
			close(done)
			close(finish)

		}
	}
}
