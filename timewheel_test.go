package timewheel_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/ydmxcz/timewheel"
)

func TestTimeWheel(t *testing.T) {
	dq := timewheel.NewTimingWheel(time.Millisecond, 1000)
	dq.Start()
	now := time.Now()
	dq.AfterFunc(time.Second, func() {
		fmt.Println(time.Now().Sub(now))
	})
	dq.AfterFunc(time.Second, func() {
		fmt.Println(time.Now().Sub(now))
	})
	dq.AfterFunc(time.Second, func() {
		fmt.Println(time.Now().Sub(now))
	})
	dq.AfterFunc(time.Second, func() {
		fmt.Println(time.Now().Sub(now))
	})
	dq.AfterFunc(time.Second, func() {
		fmt.Println(time.Now().Sub(now))
	})

	time.Sleep(time.Second * 6)

}
