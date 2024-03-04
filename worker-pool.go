package kafkawrapper

import (
	"context"
	"errors"
	"fmt"
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
	defer close(concurrentRunFunction)

	for {
		select {
		case <-ctx.Done():
			return
		default:

			fmt.Println("*** wait for read msg")

			res := <-w.workQueue
			w.workQueue <- res

			concurrentRunFunction <- struct{}{}
			fmt.Println("!!! run func")

			done := make(chan struct{}, 1)

			go func() {
				defer func() {
					if r := recover(); r != nil {
						// Recover from panic
						fmt.Println("Panic recovered:", r)

					}
				}()
				// Execute the worker function
				w.function(ctx, w.workQueue, w.resultQueue, w.errorChannel, done)
				// Close the done channel when the worker function completes
			}()

			fmt.Println("timeout vs done")
			select {

			case <-done:
				fmt.Println("done")
				<-concurrentRunFunction
				close(done)

			case <-time.After(5 * time.Second):
				fmt.Println("timeout")

				w.errorChannel <- errors.New("timeout")
				<-concurrentRunFunction
				close(done)

			}

		}
	}
}
