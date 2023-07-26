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

package cluster

import (
	"sync"
)

type node struct {
	current   int
	effective int
	endpoint  Endpoint
}

type Balancer struct {
	rss    []*node
	rsm    map[string]int
	broker *Broker
	mu     sync.RWMutex
}

// NewBalancer
func NewBalancer(broker *Broker) (b *Balancer) {
	b = &Balancer{
		broker: broker,
		rss:    make([]*node, 0),
		rsm:    make(map[string]int),
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

// Feedback
func (b *Balancer) Feedback(addr string, healthy bool) {
	if i, ok := b.rsm[addr]; !ok {
		return
	} else if node := b.rss[i]; node != nil {
		if healthy {
			node.effective++
		} else {
			node.effective--
		}
	}
}

// update
func (b *Balancer) update() {
	if 0 < len(b.rss) {
		return
	}
	for _, endpoint := range b.broker.Endpoints() {
		b.rss = append(b.rss, &node{endpoint: endpoint, effective: endpoint.Weight})
	}
	for i, node := range b.rss {
		b.rsm[node.endpoint.Addr] = i
	}
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

// IsDeprecated
func (b *Balancer) IsDeprecated(addr string) (yes bool) {
	if _, ok := b.rsm[addr]; !ok {
		yes = true
	}
	return
}
