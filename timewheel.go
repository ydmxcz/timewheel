package timewheel

import (
	"log"
	"sync/atomic"
	"time"

	"github.com/alphadose/haxmap"
)

// type location struct {
// 	slot  int
// 	etask *queueNode[task]
// }

// TimeWheel can execute job after waiting given duration
type TimeWheel struct {
	interval    time.Duration
	ticker      *time.Ticker
	slots       []*queue[task]
	timer       *haxmap.Map[string, *queueNode[task]]
	currentPos  int
	slotNum     int
	stopChannel chan bool
}

type task struct {
	delay  time.Duration
	circle int
	key    string
	job    func()
	del    int64
}

// New creates a new time wheel
func New(interval time.Duration, slotNum int) *TimeWheel {
	if interval <= 0 || slotNum <= 0 {
		return nil
	}
	tw := &TimeWheel{
		interval:    interval,
		slots:       make([]*queue[task], slotNum),
		timer:       haxmap.New[string, *queueNode[task]](8),
		currentPos:  0,
		slotNum:     slotNum,
		stopChannel: make(chan bool),
	}
	tw.initSlots()

	return tw
}

func (tw *TimeWheel) initSlots() {
	for i := 0; i < tw.slotNum; i++ {
		tw.slots[i] = &queue[task]{}
	}
}

// Start starts ticker for time wheel
func (tw *TimeWheel) Start() {
	tw.ticker = time.NewTicker(tw.interval)
	go tw.start()
}

// Stop stops the time wheel
func (tw *TimeWheel) Stop() {
	tw.stopChannel <- true
}

// AddJob add new job into pending queue
func (tw *TimeWheel) AddJob(delay time.Duration, key string, job func()) {
	if delay < 0 {
		return
	}
	tw.addTask(task{delay: delay, key: key, job: job})
}

// RemoveJob add remove job from pending queue
// if job is done or not found, then nothing happened
func (tw *TimeWheel) RemoveJob(key string) {
	if key == "" {
		return
	}
	tw.removeTask(key)
}

func (tw *TimeWheel) start() {
	for {
		select {
		case <-tw.ticker.C:
			tw.tickHandler()
		case <-tw.stopChannel:
			tw.ticker.Stop()
			return
		}
	}
}

func (tw *TimeWheel) tickHandler() {
	l := tw.slots[tw.currentPos]
	if tw.currentPos == tw.slotNum-1 {
		tw.currentPos = 0
	} else {
		tw.currentPos++
	}
	go tw.scanAndRunTask(l)
}

func (tw *TimeWheel) scanAndRunTask(l *queue[task]) {
	for {
		n := l.pop()
		if atomic.LoadInt64(&n.val.del) == 1 {
			continue
		}
		task := n.val
		if task.circle > 0 {
			task.circle--
			l.pushNode(n)
			continue
		}
		go func() {
			defer func() {
				if err := recover(); err != nil {
					log.Fatal(err)
				}
			}()
			task.job()
		}()
		if task.key != "" {
			tw.timer.Del(task.key)
		}
	}
}

func (tw *TimeWheel) addTask(task task) {
	pos, circle := tw.getPositionAndCircle(task.delay)
	task.circle = circle

	e := tw.slots[pos].push(task)
	if task.key != "" {
		_, ok := tw.timer.Get(task.key)
		if ok {
			tw.removeTask(task.key)
		}
	}
	tw.timer.Set(task.key, e)
}

func (tw *TimeWheel) getPositionAndCircle(d time.Duration) (pos int, circle int) {
	delaySeconds := int(d.Seconds())
	intervalSeconds := int(tw.interval.Seconds())
	circle = int(delaySeconds / intervalSeconds / tw.slotNum)
	pos = int(tw.currentPos+delaySeconds/intervalSeconds) % tw.slotNum

	return
}

func (tw *TimeWheel) removeTask(key string) {
	pos, ok := tw.timer.Get(key)
	if !ok {
		return
	}
	atomic.StoreInt64(&pos.val.del, 1)
	tw.timer.Del(key)
}
