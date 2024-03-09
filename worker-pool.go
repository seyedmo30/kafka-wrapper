package kafkawrapper

import (
	"context"
	"errors"
	"time"
)

type worker struct {
	id                    int
	name                  string
	optionalConfiguration OptionalConfiguration
	function              FirstClassFunc
	workQueue             chan ReadMessageDTO
	resultQueue           chan WriteMessageDTO
	errorChannel          chan error
}

func newWorker(id int, nameWorker string, fn FirstClassFunc, workQueue chan ReadMessageDTO, resultQueue chan WriteMessageDTO, errorChannel chan error, optionalConfiguration OptionalConfiguration) *worker {
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

			for {

				// "ReadMessageDTO chan is empty"
				if len(w.workQueue) != 0 {
					break
				}
				time.Sleep(time.Millisecond * 100)
			}
			concurrentRunFunction <- struct{}{}
			logger.Debug("get signal for start run func", "name_worker", w.name)

			done := make(chan struct{}, 1)

			go func() {
				defer func() {
					if r := recover(); r != nil {
						logger.Warn("Panic recovered", "external_error", r)
					}
				}()
				// Execute the worker function
				logger.Debug("start firstfunc ", "name_worker", w.name)

				w.function(ctx, w.workQueue, w.resultQueue, w.errorChannel, done)
				// Close the done channel when the worker function completes
			}()

			select {

			case <-done:
				logger.Debug("send firstfunc complete signal ", "name_worker", w.name)

				<-concurrentRunFunction
				close(done)

			case <-time.After(time.Duration(w.optionalConfiguration.Timeout) * time.Second):
				logger.Debug("timeout firstfunc", "name_worker", w.name)

				w.errorChannel <- errors.New("timeout firstfunc")
				<-concurrentRunFunction
				close(done)

			}

		}
	}
}
