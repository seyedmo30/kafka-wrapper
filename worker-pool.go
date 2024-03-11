package kafkawrapper

import (
	"context"
	"errors"
	"time"
)

type worker struct {
	function              FirstClassFunc
	functionOnlyConsumer  FirstClassFuncOnlyConsumer
	workQueue             chan ReadMessageDTO
	resultQueue           chan WriteMessageDTO
	id                    uint8
	name                  string
	errorChannel          chan error
	optionalConfiguration OptionalConfiguration
}

func newWorker(id uint8, nameWorker string, fn FirstClassFunc, workQueue chan ReadMessageDTO, resultQueue chan WriteMessageDTO, errorChannel chan error, optionalConfiguration OptionalConfiguration) *worker {
	return &worker{

		id:                    id,
		name:                  nameWorker,
		optionalConfiguration: optionalConfiguration,
		errorChannel:          errorChannel,

		function:    fn,
		workQueue:   workQueue,
		resultQueue: resultQueue,
	}
}

func newWorkerOnlyConsumer(id uint8, nameWorker string, fn FirstClassFuncOnlyConsumer, workQueue chan ReadMessageDTO, errorChannel chan error, optionalConfiguration OptionalConfiguration) *worker {
	return &worker{

		id:                    id,
		name:                  nameWorker,
		optionalConfiguration: optionalConfiguration,
		errorChannel:          errorChannel,

		functionOnlyConsumer: fn,
		workQueue:            workQueue,
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

			response := make(chan ResponseDTO)

			go func() {
				defer func() {
					if r := recover(); r != nil {
						logger.Warn("Panic recovered", "external_error", r)
					}
				}()
				// Execute the worker function
				logger.Debug("start firstfunc ", "name_worker", w.name)

				if w.functionOnlyConsumer != nil {
					w.functionOnlyConsumer(ctx, w.workQueue, w.errorChannel, response)

				}

				if w.function != nil {

					w.function(ctx, w.workQueue, w.resultQueue, w.errorChannel, response)
				}
				// Close the done channel when the worker function completes
			}()

			select {

			case res := <-response:
				logger.Debug("send firstfunc signal ", "name_worker", w.name)

				if !res.isSuccess && res.readMessageDTO.Retry < w.optionalConfiguration.Retry {
					res.readMessageDTO.Retry++
					logger.Warn("retry ", "name_worker", w.name, "msg", string(res.readMessageDTO.Value), "retry", res.readMessageDTO.Retry)
					w.workQueue <- res.readMessageDTO

				}
				<-concurrentRunFunction
				close(response)

				continue

			case <-time.After(time.Duration(w.optionalConfiguration.Timeout) * time.Second):
				logger.Debug("timeout firstfunc", "name_worker", w.name)

				w.errorChannel <- errors.New("timeout firstfunc")
				<-concurrentRunFunction
				close(response)

			}

			logger.Debug("len of channels : ", "workQueue", len(w.workQueue), "response", len(response), "resultQueue", len(w.resultQueue), "err", len(w.errorChannel))

		}
	}
}
