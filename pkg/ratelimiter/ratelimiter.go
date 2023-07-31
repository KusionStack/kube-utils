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

package ratelimiter

import (
	"sync"
	"time"

	"golang.org/x/time/rate"
	"k8s.io/client-go/util/workqueue"
)

const (
	houseKeepInterval = 20 * time.Minute
)

// Implement from workqueue.ItemBucketRateLimiter, because we need to enhance the forget method.
type ItemBucketRateLimiter struct {
	r     rate.Limit
	burst int

	limitersLock             sync.Mutex
	limiters                 map[interface{}]*rate.Limiter
	effectiveDelayTimestamps map[interface{}]time.Time
}

var _ workqueue.RateLimiter = &ItemBucketRateLimiter{}

// NewItemBucketRateLimiter creates new ItemBucketRateLimiter instance.
func NewItemBucketRateLimiter(r rate.Limit, burst int) *ItemBucketRateLimiter {
	limiter := &ItemBucketRateLimiter{
		r:                        r,
		burst:                    burst,
		limiters:                 make(map[interface{}]*rate.Limiter),
		effectiveDelayTimestamps: make(map[interface{}]time.Time),
	}
	limiter.houseKeeping()

	return limiter
}

// When returns a time.Duration which we need to wait before item is processed.
func (r *ItemBucketRateLimiter) When(item interface{}) time.Duration {
	r.limitersLock.Lock()
	defer r.limitersLock.Unlock()

	limiter, ok := r.limiters[item]
	if !ok {
		limiter = rate.NewLimiter(r.r, r.burst)
		r.limiters[item] = limiter
		r.effectiveDelayTimestamps[item] = time.Now()
	}

	reserve := limiter.Reserve()

	effectiveDelayTimestamp := r.effectiveDelayTimestamps[item]
	delay := reserve.Delay()
	now := time.Now()
	currentDelayTimestamp := now.Add(delay)
	if now.Before(effectiveDelayTimestamp) {
		// delayQueue has delaying event
		if currentDelayTimestamp.Before(effectiveDelayTimestamp) {
			// this event will replace delaying event
			r.effectiveDelayTimestamps[item] = currentDelayTimestamp
		} else {
			// This event should be dropped by DelayingQueue, so cancel it.
			reserve.Cancel()
		}
	} else {
		// this event will enqueue
		r.effectiveDelayTimestamps[item] = currentDelayTimestamp
	}
	return delay
}

// NumRequeues returns always 0 (doesn't apply to ItemBucketRateLimiter).
func (r *ItemBucketRateLimiter) NumRequeues(item interface{}) int {
	return 0
}

// Forget removes item from the internal state.
func (r *ItemBucketRateLimiter) Forget(item interface{}) {
}

func (r *ItemBucketRateLimiter) houseKeeping() {
	ticker := time.Tick(houseKeepInterval)

	go func() {
		for {
			select {
			case _ = <-ticker:
				func() {
					r.limitersLock.Lock()
					defer r.limitersLock.Unlock()

					r.limiters = make(map[interface{}]*rate.Limiter)
				}()
			}
		}
	}()
}

func NewBucketRateLimiter(r rate.Limit, burst int) *BucketRateLimiter {
	return &BucketRateLimiter{Limiter: rate.NewLimiter(r, burst), effectiveDelayTimestamps: make(map[interface{}]time.Time)}
}

type BucketRateLimiter struct {
	*rate.Limiter

	limiterLock              sync.Mutex
	effectiveDelayTimestamps map[interface{}]time.Time
}

var _ workqueue.RateLimiter = &BucketRateLimiter{}

func (r *BucketRateLimiter) When(item interface{}) time.Duration {
	r.limiterLock.Lock()
	defer r.limiterLock.Unlock()

	reserve := r.Reserve()

	effectiveDelayTimestamp, ok := r.effectiveDelayTimestamps[item]
	if !ok {
		effectiveDelayTimestamp = time.Now()
	}

	delay := reserve.Delay()
	now := time.Now()
	currentDelayTimestamp := now.Add(delay)
	if now.Before(effectiveDelayTimestamp) {
		// delayQueue has delaying event
		if currentDelayTimestamp.Before(effectiveDelayTimestamp) {
			// this event will replace delaying event
			r.effectiveDelayTimestamps[item] = currentDelayTimestamp
		} else {
			// This event should be dropped by DelayingQueue, so cancel it.
			reserve.Cancel()
		}
	} else {
		// this event will enqueue
		r.effectiveDelayTimestamps[item] = currentDelayTimestamp
	}
	return delay
}

func (r *BucketRateLimiter) NumRequeues(item interface{}) int {
	return 0
}

func (r *BucketRateLimiter) Forget(item interface{}) {
}
