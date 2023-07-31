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
	"fmt"
	"testing"
	"time"

	"k8s.io/client-go/util/workqueue"
)

func TestItemRateLimiter(t *testing.T) {
	var records []string

	queue := workqueue.NewRateLimitingQueue(NewItemBucketRateLimiter(0.5, 1))
	go func() {
		for {
			o, _ := queue.Get()
			fmt.Printf("process %s when %v\n", o, time.Now())
			records = append(records, o.(string))
			queue.Done(o)
		}
	}()

	ticker := time.Tick(1 * time.Second)
	timeout := time.After(10 * time.Second)
	object := "test"

LOOP:
	for {
		select {
		case _ = <-ticker:
			queue.AddRateLimited(object)
		case _ = <-timeout:
			break LOOP
		}
	}

	expect := 5
	fmt.Printf("expected more than %d, got %d\n", expect, len(records))
	if len(records) != expect {
		t.Fatalf("expect more than %d, got %d", expect, len(records))
	}
}

func TestRateLimiter(t *testing.T) {
	var records []string

	queue := workqueue.NewRateLimitingQueue(NewBucketRateLimiter(0.5, 1))
	go func() {
		for {
			o, _ := queue.Get()
			fmt.Printf("process %s when %v\n", o, time.Now())
			records = append(records, o.(string))
			queue.Done(o)
		}
	}()

	ticker := time.Tick(1 * time.Second)
	timeout := time.After(10 * time.Second)
	object := "test"
	count := 0

LOOP:
	for {
		select {
		case _ = <-ticker:
			queue.AddRateLimited(fmt.Sprintf("%s-%d", object, count))
			count++
		case _ = <-timeout:
			break LOOP
		}
	}

	expect := 5
	fmt.Printf("expected more than %d, got %d\n", expect, len(records))
	if len(records) != expect {
		t.Fatalf("expect more than %d, got %d", expect, len(records))
	}
}
