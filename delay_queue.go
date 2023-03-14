package timewheel

import (
	"container/heap"
	"sync"
	"sync/atomic"
	"time"
)

// 延迟队列
type DelayQueue struct {
	C chan interface{} // 有元素过期时的通知

	mu sync.Mutex    // 互斥锁
	pq priorityQueue // 优先队列

	sleeping int32         // 已休眠
	wakeupC  chan struct{} // 唤醒队列的通知
}

func NewDelayQueue(size int) *DelayQueue {

	return &DelayQueue{
		C:       make(chan interface{}), // 无缓冲管道
		pq:      newPriorityQueue(size), // 优先队列
		wakeupC: make(chan struct{}),    // 无缓冲管道saw
	}
}

// 添加元素到队列
func (dq *DelayQueue) Offer(elem interface{}, expiration int64) {
	item := &item{Value: elem, Priority: expiration} //过期时间作为优先级，过期时间越小的优先级越高

	dq.mu.Lock()
	heap.Push(&dq.pq, item) // 将元素放到队尾，并递归与父节点做比较
	index := item.Index
	dq.mu.Unlock()

	if index == 0 {
		// 如果延迟队列为休眠状态，唤醒他
		if atomic.CompareAndSwapInt32(&dq.sleeping, 1, 0) {
			// 唤醒可能会发生阻塞
			dq.wakeupC <- struct{}{}
		}
	}
}

// Poll启动一个无限循环，在这个循环中它不断地等待一个元素过期，然后将过期的元素发送到通道C。
func (dq *DelayQueue) Poll(exitC chan struct{}, nowF func() int64) {
	for {
		now := nowF()

		dq.mu.Lock()
		item, delta := dq.pq.PeekAndShift(now) //与最小堆的堆顶比较

		if item == nil {
			//没有要过期的定时器，	将延迟队列设置为休眠
			//为什么要用atomic原子函数，是为了防止Offer 和 Poll出现竞争
			atomic.StoreInt32(&dq.sleeping, 1)
		}
		dq.mu.Unlock()

		if item == nil {
			if delta == 0 {
				// 说明延迟队列中已经没有timer，因此等待新的timer添加时wake up通知，或者等待退出通知
				select {
				case <-dq.wakeupC:
					continue
				case <-exitC:
					goto exit
				}
			} else if delta > 0 {
				// 说明延迟队列中存在未过期的定时器
				select {
				case <-dq.wakeupC:
					// 当前定时器已经是休眠状态，如果添加了一个比延迟队列中最早过期的定时器更早的定时器,延迟队列被唤醒
					continue
				case <-time.After(time.Duration(delta) * time.Millisecond):
					// timer.After添加了一个相对时间定时器,并等待到期

					if atomic.SwapInt32(&dq.sleeping, 0) == 0 {
						//防止被阻塞
						<-dq.wakeupC
					}
					continue
				case <-exitC:
					goto exit
				}
			}
		}

		select {
		case dq.C <- item.Value:
		case <-exitC:
			goto exit
		}
	}

exit:
	// Reset the states
	atomic.StoreInt32(&dq.sleeping, 0)
}
