package observer

import (
	"context"
	"time"
)

type EventHandler[T any] func(ctx context.Context, events []T)

type Observer[T any] struct {
	queue   *queue[T]
	handler *handler[T]
}

func NewObserver[T any](ctx context.Context, fn EventHandler[T]) *Observer[T] {
	queue := newQueue[T]()

	o := &Observer[T]{
		queue:   queue,
		handler: newHandler(queue, fn),
	}
	go o.handler.listen(ctx)

	return o
}

func (o *Observer[T]) WithTick(tick time.Duration) *Observer[T] {
	o.handler.withTick(tick)
	return o
}

func (o *Observer[T]) Dispatch(event T) {
	o.queue.Enqueue(event)
}

func (o *Observer[T]) Flush() {
	o.handler.flush()
}

func (o *Observer[T]) Wait(ctx context.Context) {
	// タイムアウト付きのコンテキストを作成
	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	done := make(chan struct{})
	go func() {
		o.handler.flushAndWait()
		close(done)
	}()

	select {
	case <-waitCtx.Done():
		// タイムアウトまたは親コンテキストがキャンセルされた場合
		return
	case <-done:
		// 正常に完了
		return
	}
}
