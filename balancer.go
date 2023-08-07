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

package matrix

import (
	"sync"
	"time"
)

type BalancerOption func(b *Balancer)

// WithBalancerEndpointsCacheInterval
func WithBalancerEndpointsCacheInterval(interval time.Duration) BalancerOption {
	return func(b *Balancer) { b.interval = interval }
}

type node struct {
	current   int
	effective int
	endpoint  Endpoint
}

type Balancer struct {
	rss      []*node
	rsm      map[string]int
	broker   *Broker
	last     time.Time
	interval time.Duration
	mu       sync.RWMutex
}

// NewBalancer
func NewBalancer(broker *Broker, opts ...BalancerOption) (b *Balancer) {
	b = &Balancer{
		broker: broker,
		rss:    make([]*node, 0),
		rsm:    make(map[string]int),
	}
	// Set options
	for _, setOpt := range opts {
		setOpt(b)
	}
	// Option: interval
	if b.interval <= 0 {
		b.interval = 5 * time.Second
	}

	return
}

// Next
func (b *Balancer) Next() (addr string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Update rss
	b.update()

	// Next
	return b.next()
}

// Unhealth
func (b *Balancer) Unhealth(addr string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if node, ok := b.node(addr); ok && node.effective > 0 {
		node.effective = (int)(node.effective / 2)
	}
}

// IsDeprecated
func (b *Balancer) IsDeprecated(addr string) (yes bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	_, ok := b.node(addr)
	return !ok
}

// update
func (b *Balancer) update() {
	now := time.Now()
	if len(b.rss) > 0 && now.Sub(b.last) < b.interval {
		return
	}
	// Get endpoints and update
	var (
		rss = make([]*node, 0)
		rsm = make(map[string]int)
	)
	for _, endpoint := range b.broker.Endpoints() {
		node := &node{
			endpoint:  endpoint,
			effective: endpoint.Weight,
		}
		if i, ok := b.rsm[endpoint.Addr]; ok {
			node.current = b.rss[i].current
			node.effective = b.rss[i].effective
		}
		rss = append(rss, node)
	}
	for i, node := range rss {
		rsm[node.endpoint.Addr] = i
	}
	// Replace
	b.rss, b.rsm, b.last = rss, rsm, now
}

// next
func (b *Balancer) next() (addr string) {
	var (
		total int
		best  *node
	)
	for i := 0; i < len(b.rss); i++ {
		node := b.rss[i]
		total += node.effective
		node.current += node.effective
		// -1 when the connection is abnormal,
		// +1 when the communication is successful
		if node.effective < node.endpoint.Weight {
			node.effective++
		}
		// Maximum temporary weight node
		if best == nil || node.current > best.current {
			best = node
		}
	}
	if best != nil {
		best.current -= total
		addr = best.endpoint.Addr
	}
	return
}

// node
func (b *Balancer) node(addr string) (node *node, ok bool) {
	if i, ok1 := b.rsm[addr]; !ok1 {
		return nil, false
	} else if i < len(b.rss) {
		node, ok = b.rss[i], true
	}
	return
}
