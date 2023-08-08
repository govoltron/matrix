// Copyright 2023 Kami
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package balance

import (
	"sync"
	"sync/atomic"
)

type RoundRobinBalancer struct {
	rss []string
	rsm map[string]int
	cu  uint32
	mu  sync.RWMutex
}

// NewRoundRobinBalancer
func NewRoundRobinBalancer() (b *RoundRobinBalancer) {
	return &RoundRobinBalancer{
		rss: make([]string, 0),
		rsm: make(map[string]int),
	}
}

// Next implements Balancer.
func (b *RoundRobinBalancer) Next() (addr string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.rss[int(atomic.AddUint32(&b.cu, 1))%len(b.rss)]
}

// Set implements Balancer.
func (b *RoundRobinBalancer) Set(addrs []string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	// Update
	b.update(addrs)
}

// Add implements Balancer.
func (b *RoundRobinBalancer) Add(addrs ...string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	// Update
	b.update(append(addrs, b.rss...))
}

// Remove implements Balancer.
func (b *RoundRobinBalancer) Remove(addr string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	// Remove
	if i, ok := b.rsm[addr]; !ok {
		return
	} else if i < len(b.rss) {
		if i+1 == len(b.rss) {
			b.rss = b.rss[0:i]
		} else {
			b.rss = append(b.rss[0:i], b.rss[i+1:]...)
			for j := i; j < len(b.rss); i++ {
				b.rsm[b.rss[j]] = j
			}
		}
		delete(b.rsm, addr)
	}
}

// IsDeprecated implements Balancer.
func (b *RoundRobinBalancer) IsDeprecated(addr string) (yes bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	_, ok := b.rsm[addr]
	return !ok
}

// update
func (b *RoundRobinBalancer) update(addrs []string) {
	var (
		rsm = make(map[string]int)
		rss = make([]string, len(addrs))
	)
	for i, addr := range addrs {
		rss[i], rsm[addr] = addr, i
	}
	b.rss, b.rsm = rss, rsm
}
