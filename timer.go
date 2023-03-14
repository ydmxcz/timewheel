package timewheel

import (
	"container/list"
	"sync"
	"sync/atomic"
	"unsafe"
)

// Timer表示单个事件。当Timer超时时，给定的任务将被执行。
type Timer struct {
	expiration int64  // 以毫秒为单位
	task       func() //任务

	b unsafe.Pointer // 所属bucket的指针

	element *list.Element // bucket中timers双向链表中的元素
}

// 获取定时器所属的bucket
func (t *Timer) getBucket() *bucket {
	return (*bucket)(atomic.LoadPointer(&t.b))
}

// 设置定时器所属的bucket
func (t *Timer) setBucket(b *bucket) {
	atomic.StorePointer(&t.b, unsafe.Pointer(b))
}

// 阻止定时器启动
func (t *Timer) Stop() bool {
	stopped := false
	for b := t.getBucket(); b != nil; b = t.getBucket() {
		//从bucket（时间格）中移除定时器
		stopped = b.Remove(t)
	}
	return stopped
}

// 时间格
type bucket struct {
	expiration int64 // 过期时间

	mu     sync.Mutex //互斥锁
	timers *list.List //定时器链表（双向链表）
}

// new一个时间格
func newBucket() *bucket {
	return &bucket{
		timers:     list.New(),
		expiration: -1, //过期时间默认为-1
	}
}

// 获取过期时间
func (b *bucket) Expiration() int64 {
	return atomic.LoadInt64(&b.expiration)
}

// 设置过期时间
func (b *bucket) SetExpiration(expiration int64) bool {
	return atomic.SwapInt64(&b.expiration, expiration) != expiration
}

// 添加定时器
func (b *bucket) Add(t *Timer) {
	b.mu.Lock()

	e := b.timers.PushBack(t)
	t.setBucket(b)
	t.element = e

	b.mu.Unlock()
}

// 删除定时器
func (b *bucket) remove(t *Timer) bool {
	if t.getBucket() != b {
		//如果定时器所属的bucket不是当前的bucket返回false
		return false
	}
	b.timers.Remove(t.element)
	t.setBucket(nil)
	t.element = nil
	return true
}

func (b *bucket) Remove(t *Timer) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.remove(t)
}

// 刷新
// 1将定时器链表中的定时器全部清空
// 2将定时器链表中的定时器放入到ts切片中（ts = time slice）
// 3将bucket过期时间设置成-1
// 4循环遍历ts切片调用addOrRun方法
func (b *bucket) Flush(reinsert func(*Timer)) {
	var ts []*Timer

	b.mu.Lock()
	//将定时器链表中的定时器全部删除，并放到ts切片中
	for e := b.timers.Front(); e != nil; {
		next := e.Next()

		t := e.Value.(*Timer)
		b.remove(t)
		ts = append(ts, t)

		e = next
	}
	b.mu.Unlock()
	//将bucket的到期时间重新设置成-1
	b.SetExpiration(-1) // TODO: Improve the coordination with b.Add()

	for _, t := range ts {
		reinsert(t)
	}
}
