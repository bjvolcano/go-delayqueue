/**
 * @Author bjvolcano
 * @Date 2021/1/22 下午5:08
 * @Version 1.0
 */
package queue

import (
	"sort"
	"sync"
	"time"
)

var (
	ADD         = 1
	POP         = 2
	EnqueueTime = time.Second * 3
	GCTime      = time.Second * 3
)

type DealyObjectSorter struct {
	items     []delayObject
	SyncItems sync.RWMutex
}

func NewSorter(m []delayObject) *DealyObjectSorter {
	sorter := DealyObjectSorter{items: m}
	return &sorter
}
func (this *DealyObjectSorter) Len() int {
	return len(this.items)
}

func (this *DealyObjectSorter) Sort() {
	if this.Len() > 1 {
		this.SyncItems.Lock()
		defer this.SyncItems.Unlock()
		sort.Sort(this)
	}
}
func (this *DealyObjectSorter) Add(item delayObject) {
	this.SyncItems.Lock()
	defer this.SyncItems.Unlock()
	this.items = append(this.items, item)

}
func (this *DealyObjectSorter) Less(i, j int) bool {
	return this.items[i].delayTime.Before(this.items[j].delayTime) // 按值排序
	//return ms[i].Key < ms[j].Key // 按键排序
}
func (this *DealyObjectSorter) Swap(i, j int) {
	this.items[i], this.items[j] = this.items[j], this.items[i]
}

func (this *DealyObjectSorter) Pop() *delayObject {
	if len(this.items) == 0 {
		return nil
	}
	this.SyncItems.Lock()
	defer this.SyncItems.Unlock()
	head := this.items[0]
	this.items = this.items[1:]
	return &head
}

func (this *DealyObjectSorter) Peek() *delayObject {
	if len(this.items) > 0 {
		this.SyncItems.RLock()
		defer this.SyncItems.RUnlock()
		return &this.items[0]
	}
	return nil
}

func (this *DealyObjectSorter) Items() []delayObject {
	return this.items
}

//延迟元素的封装对象
type delayObject struct {
	delay time.Duration
	//通过delay计算的延迟时间
	delayTime time.Time
	val       interface{}
}

func NewDelayQueue() *delayQueue {
	var delayQueue = delayQueue{}
	delayQueue.init()
	return &delayQueue
}

type delayQueue struct {

	/*
		item array flag chan
	*/
	hasItem chan bool
	//
	sorted chan bool

	//sorted array
	sorter *DealyObjectSorter

	//delays []delayObject
	items []interface{}

	itemSync sync.RWMutex

	initSync sync.Mutex
	run      bool

	lastTime time.Time

	ticker *time.Ticker
}

func (this *delayQueue) Put(delayTime time.Duration, val interface{}) {

	this.addDelayObject(delayTime, val)
}

func (this *delayQueue) addDelayObject(delay time.Duration, val interface{}) {
	this.lastTime = time.Now()
	delayTime := time.Now().Add(delay)
	this.sorter.Add(delayObject{delayTime: delayTime, delay: delay, val: val})
}

func (this *delayQueue) notify(ntype int) {
	if ntype == ADD {
		this.sorted <- true
	} else if ntype == POP {

		//todo 是否无元素时通知

		//if len(this.items) == 0 {
		//	this.noItem <- true
		//}
		//if len(this.delays) == 0 {
		//	this.noDelay <- true
		//}
	}
}

func (this *delayQueue) Take() interface{} {
	return this.Pop()
}

func (this *delayQueue) Pop() interface{} {
	for {
		select {
		case <-this.hasItem:
			head := this.pop()
			if head != nil {
				go this.notify(POP)
				return head
			}
		}
	}
}

func (this *delayQueue) pop() interface{} {
	if len(this.items) > 0 {
		this.itemSync.Lock()
		defer this.itemSync.Unlock()
		head := this.items[0]
		this.items = this.items[1:]
		return head
	}
	return nil
}

func (this *delayQueue) popSortedItem() *delayObject {
	//	根据到期时间排序
	//调整延迟数组
	//弹出最先到时间的元素 并且在队列中移除该元素

	return this.sorter.Pop()

}

func (this *delayQueue) init() {
	if !this.run {
		this.initSync.Lock()
		defer this.initSync.Unlock()
		if !this.run {
			this.hasItem = make(chan bool)
			this.sorted = make(chan bool)

			this.ticker = time.NewTicker(time.Millisecond * 10)

			this.run = true
			this.sorter = NewSorter([]delayObject{})
			this.lastTime = time.Now()
			go this.startDelayLoop()
		}

	}
}
func (this *delayQueue) signalLoop() {
	for this.run {
		select {
		case <-this.ticker.C:

			if len(this.items) > 0 {
				this.hasItem <- true
			}
		}
	}
}

func (this *delayQueue) sortLoop() {

	for this.run {
		select {
		case <-this.ticker.C:
			if this.sorter.Len() > 0 {
				this.sorter.Sort()
				this.notify(ADD)
			}
		}
	}
}

//startDelayLoop:开启延时检测
func (this *delayQueue) startDelayLoop() {
	go this.signalLoop()
	go this.sortLoop()
	for this.run {
		select {
		case <-this.sorted:
			//排序，最小时间排最前
			go this.addItem()
		}
	}
}

func (this *delayQueue) appendItem(item *delayObject) {

	this.itemSync.Lock()
	defer this.itemSync.Unlock()
	this.items = append(this.items, item.val)

}

func (this *delayQueue) addItem() {

	var (
		item *delayObject
		now  time.Time
		sub  time.Duration
	)
	item = this.popSortedItem()

	if item == nil {
		return
	}
	now = time.Now()

	//队列延时已经足够或者小于当前时间，直接入队
	if now.After(item.delayTime) {
		this.appendItem(item)
		return
	}

	sub = item.delayTime.Sub(now)
	if sub < (EnqueueTime) {
		<-time.NewTicker(sub).C
		this.appendItem(item)
	} else {
		this.sorter.Add(delayObject{delayTime: item.delayTime, delay: item.delay, val: item.val})
	}
}

func (this *delayQueue) Shutdown() {

	if this.run && len(this.items) == 0 && this.sorter.Len() == 0 {

		this.sorter.SyncItems.Lock()
		defer this.sorter.SyncItems.Unlock()
		this.initSync.Lock()
		defer this.initSync.Unlock()
		this.itemSync.Lock()
		defer this.itemSync.Unlock()
		this.run = false
		this.sorter = nil
		this.items = nil
		this.ticker = nil
		this.hasItem = nil

	}
}
