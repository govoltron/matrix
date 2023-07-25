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

type Balancer struct {
	workers    map[string]Endpoint
	ready      map[string]Endpoint
	deprecated map[string]Endpoint
	broker     *Broker
	mu         sync.RWMutex
}

// NewBalancer
func NewBalancer(broker *Broker) (b *Balancer) {
	b = &Balancer{
		workers:    make(map[string]Endpoint),
		ready:      make(map[string]Endpoint),
		deprecated: make(map[string]Endpoint),
		broker:     broker,
	}
	return
}

// Acquire
func (b *Balancer) Acquire() (addr string) {

	b.broker.Endpoints()

	var endpoint Endpoint
	for _, ep := range b.ready {
		endpoint = ep
		break
	}
	if endpoint.Addr != "" {
		return endpoint.Addr
	}
	return
}

// Release
func (b *Balancer) Release(addr string) {

}
