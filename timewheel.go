package timewheel

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

// 分层时间轮
type TimingWheel struct {
	tick      int64 // 每一个时间格的跨度,以毫秒为单位
	wheelSize int64 // 时间格的数量

	interval    int64       // 总的跨度数 tick * wheelSize，以毫秒为单位
	currentTime int64       // 当前指针指向的时间，以毫秒为单位
	buckets     []*bucket   //时间格列表
	queue       *DelayQueue //延迟队列,

	overflowWheel unsafe.Pointer // 上一层时间轮的指针

	exitC     chan struct{} // 退出通知
	waitGroup waitGroupWrapper
}

type waitGroupWrapper struct {
	wg sync.WaitGroup
}

func (wgw *waitGroupWrapper) Wrap(f func()) {
	go func() {
		wgw.wg.Add(1)
		f()
		wgw.wg.Done()
	}()
}

func (wgw *waitGroupWrapper) Wait() {
	wgw.wg.Wait()
}

// 对外暴露的初始化时间轮方法,参数为时间格跨度，和时间格数量
func NewTimingWheel(tick time.Duration, wheelSize int64) *TimingWheel {
	//时间格(毫秒)
	tickMs := int64(tick / time.Millisecond)
	if tickMs <= 0 {
		panic(errors.New("tick must be greater than or equal to 1ms"))
	}

	//开始时间
	startMs := time.Now().UnixMilli()

	return newTimingWheel(
		tickMs,
		wheelSize,
		startMs,
		NewDelayQueue(int(wheelSize)), //delayqueue
	)
}

func truncate(expiration, tick int64) int64 {
	return expiration //- (expiration % tick)
}

// 内部初始化时间轮的方法，参数为，时间格跨度（毫秒），时间格数量，开始时间（毫秒）, 延迟队列
func newTimingWheel(tickMs int64, wheelSize int64, startMs int64, queue *DelayQueue) *TimingWheel {
	//根据时间格数量创建时间格列表
	buckets := make([]*bucket, wheelSize)
	for i := range buckets {
		buckets[i] = newBucket()
	}

	return &TimingWheel{
		tick:        tickMs,
		wheelSize:   wheelSize,
		currentTime: truncate(startMs, tickMs),
		interval:    tickMs * wheelSize,
		buckets:     buckets,
		queue:       queue,
		exitC:       make(chan struct{}),
	}
}

// add添加定时器到时间轮
// 如果定时器已过期返回false
func (tw *TimingWheel) add(t *Timer) bool {
	//当前秒钟时间轮的currentTime = 1626333377000（2021-07-15 15:16:17）
	currentTime := atomic.LoadInt64(&tw.currentTime)
	if t.expiration < currentTime+tw.tick {
		//定时器的过期时间已经过期返回false
		return false
	} else if t.expiration < currentTime+tw.interval {
		//定时器的过期时间小于当前时间轮的当前时间+轮的总跨度，将定时器放到对应的bucket中，并将bucket放入延迟队列。

		//假设过期时间为2021-07-15 15:17:02（1626333422000）
		//1626333422000 < 1626333377000 + 60*1000
		//virtualID = 1626333422000 / 1000 = 1626333422
		//1626333422%60 = 2，将定时器放到第2个时间格中
		//设置bucket（时间格）的过期时间
		virtualID := t.expiration / tw.tick

		b := tw.buckets[virtualID%tw.wheelSize]
		b.Add(t)

		if b.SetExpiration(virtualID * tw.tick) {
			//如果设置的过期时间不等于桶的过期时间
			//将bucket添加到延迟队列，重新排序延迟队列
			tw.queue.Offer(b, b.Expiration())
		}

		return true
	} else {
		//定时器的过期时间 大于 当前时间轮的当前时间+轮的总跨度，递归将定时器添加到上一层轮。
		overflowWheel := atomic.LoadPointer(&tw.overflowWheel)
		if overflowWheel == nil {
			atomic.CompareAndSwapPointer(
				&tw.overflowWheel,
				nil,
				unsafe.Pointer(newTimingWheel(
					tw.interval,
					tw.wheelSize,
					currentTime,
					tw.queue,
				)),
			)
			overflowWheel = atomic.LoadPointer(&tw.overflowWheel)
		}
		return (*TimingWheel)(overflowWheel).add(t)
	}
}

// 执行已过期定时器的任务，将未到期的定时器重新放回时间轮
func (tw *TimingWheel) addOrRun(t *Timer) {
	if !tw.add(t) {
		go t.task()
	}
}

//推进时钟
//我们就以时钟举例:假如当前时间是2021-07-15 15:16:17（1626333375000毫秒），过期时间是2021-07-15 15:17:18（1626333438000）毫秒
//从秒轮开始,1626333438000 > 1626333377000 + 1000， truncate(1626333438000,1000)=1626333438000, 秒轮的当前时间设置为1626333438000（2021-07-15 15:17:18），有上层时间轮
//到了分钟轮 1626333438000 > 1626333377000 + 60000=1626333437000, truncate(1626333438000,60000)=1626333420000（2021-07-15 15:17:00），分轮的当前时间设置为1626333438000，有上层时间轮
//到了时钟轮 1626333438000 < 1626333377000 + 360000，时钟轮当前时间不变（2021-07-15 15:16:17），没上层时间轮

func (tw *TimingWheel) advanceClock(expiration int64) {
	currentTime := atomic.LoadInt64(&tw.currentTime)

	if expiration >= currentTime+tw.tick {

		//将过期时间截取到时间格间隔的最小整数倍
		//举例：
		//expiration = 100ms，tw.tick = 3ms, 结果 100 - 100%3 = 99ms,因此当前的时间来到了99ms，
		//目的就是找到合适的范围，比如[0,3)、[3-6)、[6,9) expiration=5ms时，currentTime=3ms。

		currentTime = truncate(expiration, tw.tick)
		atomic.StoreInt64(&tw.currentTime, currentTime)

		// 如果有上层时间轮，那么递归调用上层时间轮的引用
		overflowWheel := atomic.LoadPointer(&tw.overflowWheel)
		if overflowWheel != nil {
			(*TimingWheel)(overflowWheel).advanceClock(currentTime)
		}
	}
}

// 时间轮转起来
func (tw *TimingWheel) Start() {
	tw.waitGroup.Wrap(func() {
		//开启一个协程，死循环延迟队列，将已过期的bucket(时间格)弹出
		tw.queue.Poll(tw.exitC, func() int64 {
			return time.Now().UnixMilli() //timeToMs(time.Now().UTC())
		})
	})

	tw.waitGroup.Wrap(func() {
		for {
			select {
			//开启另外一个协程，阻塞接收延迟队列弹出的bucket(时间格)
			case elem := <-tw.queue.C:
				// 从延迟队列弹出来的是一个bucket（时间格）
				b := elem.(*bucket)
				// 时钟推进，将时钟的当前时间推进到过期时间
				tw.advanceClock(b.Expiration())
				// 将bucket（时间格）中的已到期的定时器执行，还没有到过期时间重新放回时间轮
				b.Flush(tw.addOrRun)
			case <-tw.exitC:
				return
			}
		}
	})
}

// 停止时间轮
// 关闭管道
func (tw *TimingWheel) Stop() {
	close(tw.exitC)
	tw.waitGroup.Wait()
}

// 添加定时任务到时间轮
func (tw *TimingWheel) AfterFunc(d time.Duration, f func()) *Timer {

	t := &Timer{
		expiration: time.Now().Add(d).UnixMilli(), //timeToMs(time.Now().UTC().Add(d)),
		task:       f,
	}
	tw.addOrRun(t)
	return t
}
