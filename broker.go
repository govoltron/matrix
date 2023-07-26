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
	"context"
	"sync"
	"sync/atomic"
)

type Broker struct {
	name      string
	endpoints map[string]Endpoint
	updateC   chan Endpoint
	deleteC   chan string
	watcher   *watcher
	ctx       context.Context
	cancel    context.CancelFunc
	closed    uint32
	wg        sync.WaitGroup
	mu        sync.RWMutex
	matrix    *Matrix
}

// NewBroker
func (m *Matrix) NewBroker(ctx context.Context, srvname string) (b *Broker) {
	b = &Broker{
		name:      srvname,
		endpoints: make(map[string]Endpoint),
		updateC:   make(chan Endpoint, 1),
		deleteC:   make(chan string, 1),
		matrix:    m,
	}
	// Context
	b.ctx, b.cancel = context.WithCancel(ctx)
	// Watcher
	b.watcher = &watcher{b}
	// Sync
	b.wg.Add(1)
	go b.sync()

	return
}

// sync
func (b *Broker) sync() {
	defer func() {
		b.wg.Done()
	}()

	// Watch
	b.matrix.Watch(b.ctx, b.name, b.watcher)

	for {
		select {
		// Done
		case <-b.ctx.Done():
			return
		// Update
		case endpoint := <-b.updateC:
			b.mu.Lock()
			b.endpoints[endpoint.ID] = endpoint
			b.mu.Unlock()
		// Delete
		case member := <-b.deleteC:
			b.mu.Lock()
			delete(b.endpoints, member)
			b.mu.Unlock()
		}
	}
}

// Endpoints
func (b *Broker) Endpoints() (endpoints map[string]Endpoint) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	endpoints = make(map[string]Endpoint)
	for member, endpoint := range b.endpoints {
		endpoints[member] = endpoint
	}
	return
}

// Close
func (b *Broker) Close() {
	if !atomic.CompareAndSwapUint32(&b.closed, 0, 1) {
		return
	}
	b.cancel()
	b.wg.Wait()
}

type watcher struct {
	b *Broker
}

// OnInit implements Watcher.
func (w *watcher) OnInit(endpoints map[string]Endpoint) {
	w.b.mu.Lock()
	defer w.b.mu.Unlock()
	w.b.endpoints = make(map[string]Endpoint)
	for member, endpoint := range endpoints {
		w.b.endpoints[member] = endpoint
	}
}

// OnUpdate implements Watcher.
func (w *watcher) OnUpdate(member string, endpoint Endpoint) {
	w.b.updateC <- endpoint
}

// OnDelete implements Watcher.
func (w *watcher) OnDelete(member string) {
	w.b.deleteC <- member
}
