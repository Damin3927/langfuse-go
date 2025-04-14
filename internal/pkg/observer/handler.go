package observer

import (
	"context"
	"time"
)

type command int

const (
	commanFlush command = iota
	commandFlushAndWait
	commandFlushDone
)

const (
	defaultTickerPeriod = 1 * time.Second
)

type handler[T any] struct {
	queue        *queue[T]
	fn           EventHandler[T]
	commandCh    chan command
	tickerPeriod time.Duration
}

func newHandler[T any](queue *queue[T], fn EventHandler[T]) *handler[T] {
	return &handler[T]{
		queue:        queue,
		fn:           fn,
		commandCh:    make(chan command),
		tickerPeriod: defaultTickerPeriod,
	}
}

func (h *handler[T]) withTick(period time.Duration) *handler[T] {
	h.tickerPeriod = period
	return h
}

func (h *handler[T]) listen(ctx context.Context) {
	ticker := time.NewTicker(h.tickerPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// コンテキストがキャンセルされたらgoroutineを終了
			close(h.commandCh)
			return
		case <-ticker.C:
			// バックグラウンド処理のためのコンテキストを作成
			// 親コンテキストがキャンセルされても、この処理は短時間で完了するようにタイムアウトを設定
			handleCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			h.handle(handleCtx)
			cancel()
		case cmd, ok := <-h.commandCh:
			if !ok {
				return
			}

			// 明示的なflush命令のためのコンテキスト
			handleCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			h.handle(handleCtx)
			cancel()

			if cmd == commandFlushAndWait {
				ticker.Stop()
				close(h.commandCh)
				return
			}
		}
	}
}

func (h *handler[T]) handle(ctx context.Context) {
	events := h.queue.All()
	if len(events) > 0 {
		h.fn(ctx, events)
	}
}

func (h *handler[T]) flush() {
	select {
	case h.commandCh <- commanFlush:
	default:
		// チャネルがブロックされている場合はスキップ
	}
}

func (h *handler[T]) flushAndWait() {
	select {
	case h.commandCh <- commandFlushAndWait:
		// チャネルが閉じられていない場合は待機
		_, ok := <-h.commandCh
		if !ok {
			return
		}
	default:
		// チャネルがブロックされている場合は既に終了していると判断
	}
}
