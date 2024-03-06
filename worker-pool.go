package kafkawrapper

import (
	"context"
	"errors"
	"time"
)

type worker struct {
	id                    int
	optionalConfiguration OptionalConfiguration
	function              FirstClassFunc
	workQueue             chan ReadMessageDTO
	resultQueue           chan WriteMessageDTO
	errorChannel          chan error
}

func newWorker(id int, limitRunFunction int, fn FirstClassFunc, workQueue chan ReadMessageDTO, resultQueue chan WriteMessageDTO, errorChannel chan error, optionalConfiguration OptionalConfiguration) *worker {
	return &worker{
		id:                    id,
		optionalConfiguration: optionalConfiguration,
		function:              fn,
		workQueue:             workQueue,
		resultQueue:           resultQueue,
		errorChannel:          errorChannel,
	}
}

func (w *worker) start(ctx context.Context) {
	concurrentRunFunction := make(chan struct{}, w.optionalConfiguration.NumberFuncInWorker)
	defer close(concurrentRunFunction)

	for {
		select {
		case <-ctx.Done():
			logger.Warn("context done")
			return
		default:

			logger.Debug("wait for receive message from WriteMessageDTO channel")
			res := <-w.workQueue
			w.workQueue <- res
			logger.Debug("receive message from WriteMessageDTO channel")

			concurrentRunFunction <- struct{}{}
			logger.Debug("get signal for start run func")

			done := make(chan struct{}, 1)

			go func() {
				defer func() {
					if r := recover(); r != nil {
						logger.Warn("Panic recovered", "external_error", r)
					}
				}()
				// Execute the worker function
				logger.Debug("start firstfunc ")

				w.function(ctx, w.workQueue, w.resultQueue, w.errorChannel, done)
				// Close the done channel when the worker function completes
			}()

			select {

			case <-done:
				logger.Debug("send firstfunc complete signal ")

				<-concurrentRunFunction
				close(done)

			case <-time.After(time.Duration(w.optionalConfiguration.Timeout) * time.Second):
				logger.Debug("timeout firstfunc")

				w.errorChannel <- errors.New("timeout firstfunc")
				<-concurrentRunFunction
				close(done)

			}

		}
	}
}
