/**
 * @Author bjvolcano
 * @Date 2021/1/27 上午11:27
 * @Version 1.0
 */
package queue

import (
	"fmt"
	"strconv"
	"testing"
	"time"
)

func TestDelayQueue(t *testing.T) {

	var delayQueue = NewDelayQueue()
	var i int64
	for i = 0; i < 10; i++ {
		go func(i int64) {

			for {
				fmt.Println("gorouting id ", i, "take val ", delayQueue.Take())
			}

		}(i)
	}
	go func() {
		var start int64

		test(start, delayQueue)
	}()

	go func() {
		var start int64 = 999

		test(start, delayQueue)
	}()
	var r chan bool
	r <- true
}

func test(index int64, delayQueue *delayQueue) {
	var duration = time.Second
	var i int64
	var end = index + 1000
	for i = index; i < end; i++ {
		duration += time.Millisecond * 10
		go func(duration time.Duration, temp int64) {
			var item = "       [" + strconv.Itoa(int(temp)) + "] "
			delayQueue.Put(duration, item)

			//fmt.Println("add item delay :", item)
		}(duration, i)
	}
}
