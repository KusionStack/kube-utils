/**
 * Copyright 2023 KusionStack Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package workqueue

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/util/clock"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
)

type GetPriorityFunc func(item interface{}) int

var _ workqueue.Interface = &PriorityQueue{}

type PriorityQueueConfig struct {
	Name                       string          // the name of the queue
	NumOfPriorityLotteries     []int           // the number of lotteries for each priority, priority is from low to high, higher priority must has more lotteries
	GetPriorityFunc            GetPriorityFunc // the function to get the priority of an item, should return a value between 0 and len(NumOfPriorityLotteries)-1
	UnfinishedWorkUpdatePeriod time.Duration   // the period to update the unfinished work
}

func NewPriorityQueue(cfg *PriorityQueueConfig) (*PriorityQueue, error) {
	if cfg == nil {
		return nil, fmt.Errorf("config is required")
	}

	if cfg.NumOfPriorityLotteries == nil {
		cfg.NumOfPriorityLotteries = DefaultNumOfPriorityLotteries
	}
	if cfg.GetPriorityFunc == nil {
		return nil, fmt.Errorf("GetPriorityFunc is required")
	}

	lotteries, err := getLotteries(cfg.NumOfPriorityLotteries)
	if err != nil {
		return nil, err
	}
	shuffleLotteries(lotteries)
	klog.V(1).Infof("Get priority lotteries, length: %v, sequence: %v", len(lotteries), lotteries)

	unfinishedWorkUpdatePeriod := cfg.UnfinishedWorkUpdatePeriod
	if unfinishedWorkUpdatePeriod == 0 {
		unfinishedWorkUpdatePeriod = DefaultUnfinishedWorkUpdatePeriod
	}

	if cfg.Name == "" {
		return nil, fmt.Errorf("name is required")
	}

	rc := clock.RealClock{}

	return newPriorityQueue(
		lotteries,
		len(cfg.NumOfPriorityLotteries)-1,
		cfg.GetPriorityFunc,
		rc,
		globalPriorityQueueMetricsFactory.newPriorityQueueMetrics(cfg.Name, rc),
		unfinishedWorkUpdatePeriod,
	), nil
}

func getLotteries(numOfPriorityLotteries []int) ([]int, error) {
	if len(numOfPriorityLotteries) == 0 {
		return nil, fmt.Errorf("NumOfPriorityLotteries is required")
	}

	var lotteries []int
	for i := 0; i < len(numOfPriorityLotteries); i++ {
		if numOfPriorityLotteries[i] <= 0 {
			return nil, fmt.Errorf("invalid numOfPriorityLotteries")
		}

		if i > 0 && numOfPriorityLotteries[i] < numOfPriorityLotteries[i-1] {
			return nil, fmt.Errorf("invalid numOfPriorityLotteries")
		}

		for j := 0; j < numOfPriorityLotteries[i]; j++ {
			lotteries = append(lotteries, i)
		}
	}

	return lotteries, nil
}

func shuffleLotteries(lotteries []int) {
	rand.Seed(time.Now().UnixNano())
	n := len(lotteries)
	for i := n - 1; i > 0; i-- {
		j := rand.Intn(i + 1)
		lotteries[i], lotteries[j] = lotteries[j], lotteries[i]
	}
}

func newPriorityQueue(lotteries []int, maxPriority int, f GetPriorityFunc, c clock.Clock, metrics priotityQueueMetrics, updatePeriod time.Duration) *PriorityQueue {
	t := &PriorityQueue{
		lotteries:                  lotteries,
		maxPriority:                maxPriority,
		getPriorityFunc:            f,
		priorityQueue:              make([][]t, len(lotteries)),
		clock:                      c,
		dirty:                      set{},
		processing:                 set{},
		cond:                       sync.NewCond(&sync.Mutex{}),
		metrics:                    metrics,
		unfinishedWorkUpdatePeriod: updatePeriod,
	}

	// Don't start the goroutine for a type of noMetrics so we don't consume
	// resources unnecessarily
	if _, ok := metrics.(noPriorityQueueMetrics); !ok {
		go t.updateUnfinishedWorkLoop()
	}

	return t
}

type PriorityQueue struct {
	lotteries       []int
	maxPriority     int
	getPriorityFunc GetPriorityFunc

	lotteryIndex int // lotteryIndex is the index of the lottery, it will be increased by 1 every time we get an item from the queue

	// queue defines the order in which we will work on items. Every
	// element of queue should be in the dirty set and not in the
	// processing set.
	priorityQueue [][]t

	// count is the number of items in the queue.
	count int

	// dirty defines all of the items that need to be processed.
	dirty set

	// Things that are currently being processed are in the processing set.
	// These things may be simultaneously in the dirty set. When we finish
	// processing something and remove it from this set, we'll check if
	// it's in the dirty set, and if so, add it to the queue.
	processing set

	cond                       *sync.Cond
	shuttingDown               bool
	metrics                    priotityQueueMetrics
	unfinishedWorkUpdatePeriod time.Duration
	clock                      clock.Clock
}

type empty struct{}
type t interface{}
type set map[t]empty

func (s set) has(item t) bool {
	_, exists := s[item]
	return exists
}

func (s set) insert(item t) {
	s[item] = empty{}
}

func (s set) delete(item t) {
	delete(s, item)
}

// Add marks item as needing processing.
func (q *PriorityQueue) Add(item interface{}) {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	if q.shuttingDown {
		return
	}
	if q.dirty.has(item) {
		return
	}

	priority := q.getPriority(item)
	q.metrics.add(item, priority)

	q.dirty.insert(item)
	if q.processing.has(item) {
		return
	}

	q.priorityQueue[priority] = append(q.priorityQueue[priority], item)
	q.count++
	klog.V(5).Infof("Add item to priority queue, priority: %v, count: %v", priority, q.count)
	q.cond.Signal()
}

func (q *PriorityQueue) getPriority(item interface{}) int {
	priority := q.getPriorityFunc(item)
	if priority < 0 {
		return 0
	} else if priority > q.maxPriority {
		return q.maxPriority
	}
	return priority
}

// Len returns the current queue length, for informational purposes only. You
// shouldn't e.g. gate a call to Add() or Get() on Len() being a particular
// value, that can't be synchronized properly.
func (q *PriorityQueue) Len() int {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()

	return q.lenNoLock()
}

func (q *PriorityQueue) lenNoLock() int {
	return q.count
}

// Get blocks until it can return an item to be processed. If shutdown = true,
// the caller should end their goroutine. You must call Done with item when you
// have finished processing it.
func (q *PriorityQueue) Get() (item interface{}, shutdown bool) {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()

	for q.lenNoLock() == 0 && !q.shuttingDown {
		q.cond.Wait()
	}
	if q.lenNoLock() == 0 {
		// It must be shutting down.
		return nil, true
	}

	lottery := q.nextLottery()
	for len(q.priorityQueue[lottery]) == 0 {
		lottery = q.nextLottery()
	}

	item, q.priorityQueue[lottery] = q.priorityQueue[lottery][0], q.priorityQueue[lottery][1:]
	q.count--
	klog.V(5).Infof("Get item from priority queue, lottery: %v, count: %v", lottery, q.count)

	q.metrics.get(item, lottery)

	q.processing.insert(item)
	q.dirty.delete(item)

	return item, false
}

func (q *PriorityQueue) nextLottery() int {
	q.lotteryIndex = q.lotteryIndex + 1
	if q.lotteryIndex >= len(q.lotteries) {
		q.lotteryIndex = 0
	}
	return q.lotteries[q.lotteryIndex]
}

// Done marks item as done processing, and if it has been marked as dirty again
// while it was being processed, it will be re-added to the queue for
// re-processing.
func (q *PriorityQueue) Done(item interface{}) {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()

	priority := q.getPriority(item)
	q.metrics.done(item, priority)

	q.processing.delete(item)
	if q.dirty.has(item) {
		q.priorityQueue[priority] = append(q.priorityQueue[priority], item)
		q.count++
		klog.V(5).Infof("Add item to priority queue from dirty, priority: %v, count: %v", priority, q.count)
		q.cond.Signal()
	}
}

// ShutDown will cause q to ignore all new items added to it. As soon as the
// worker goroutines have drained the existing items in the queue, they will be
// instructed to exit.
func (q *PriorityQueue) ShutDown() {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()

	q.shuttingDown = true
	q.cond.Broadcast()
}

func (q *PriorityQueue) ShuttingDown() bool {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()

	return q.shuttingDown
}

func (q *PriorityQueue) updateUnfinishedWorkLoop() {
	t := q.clock.NewTicker(q.unfinishedWorkUpdatePeriod)
	defer t.Stop()
	for range t.C() {
		if !func() bool {
			q.cond.L.Lock()
			defer q.cond.L.Unlock()
			if !q.shuttingDown {
				q.metrics.updateUnfinishedWork()
				return true
			}
			return false
		}() {
			return
		}
	}
}
