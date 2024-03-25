package kafkawrapper

import (
	"context"
	"errors"
	"runtime"
	"sync"
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
	var wgFunc sync.WaitGroup
	defer close(concurrentRunFunction)

	// go func() {

	// 	<-ctx.Done()
	// 	for {

	// 		time.Sleep(time.Second)
	// 		if len(w.workQueue) == 0 && len(w.resultQueue) == 0 && len(w.errorChannel) == 0 {
	// 			logger.Info("all channel are empty and graceful exit")
	// 			os.Exit(0)
	// 		}
	// 		logger.Debug("wait for empty channel : ", "workQueue", len(w.workQueue), "resultQueue", len(w.resultQueue), "err", len(w.errorChannel))

	// 	}
	// }()

serviceLoop:
	for {

		select {
		case <-ctx.Done():
			if len(w.workQueue) == 0 && len(w.resultQueue) == 0 && len(w.errorChannel) == 0 {
				logger.Info("all channel are empty and graceful exit")
				break serviceLoop
			}
		default:
			// Do Nothing
		}
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

			wgFunc.Add(1)

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

			if !res.IsSuccess && res.ReadMessageDTO.Retry < w.optionalConfiguration.Retry {
				res.ReadMessageDTO.Retry++
				logger.Debug("retry ", "name_worker", w.name, "msg", string(res.ReadMessageDTO.Value), "retry", res.ReadMessageDTO.Retry)
				w.workQueue <- res.ReadMessageDTO

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
		wgFunc.Done()

		logger.Debug("len of channels : ", "workQueue", len(w.workQueue), "response", len(response), "resultQueue", len(w.resultQueue), "err", len(w.errorChannel))
		var m runtime.MemStats
		runtime.ReadMemStats(&m)

		logger.Debug("htop ===>", "Alloc-MB", m.Alloc/1024/1024, "TotalAlloc-MB", m.TotalAlloc/1024/1024, "Sys-MB", m.Sys/1024/1024, "NumGC", m.NumGC, "NumGoroutine", runtime.NumGoroutine())

	}

	wgFunc.Wait()

}
