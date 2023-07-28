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

type kv struct {
	Key   string
	Value []byte
}

type Broker struct {
	srvname   string
	endpoints map[string]Endpoint
	dict      map[string][]byte
	updateMC  chan Endpoint
	deleteMC  chan string
	updateVC  chan kv
	deleteVC  chan string
	mwatcher  *memberWatcher
	vwatcher  *valueWatcher
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
		srvname:   srvname,
		endpoints: make(map[string]Endpoint),
		dict:      make(map[string][]byte),
		updateMC:  make(chan Endpoint, 1),
		deleteMC:  make(chan string, 1),
		updateVC:  make(chan kv, 1),
		deleteVC:  make(chan string, 1),
		matrix:    m,
	}
	// Context
	b.ctx, b.cancel = context.WithCancel(ctx)
	// Watcher
	b.mwatcher = &memberWatcher{b}
	b.vwatcher = &valueWatcher{b}
	// Watch
	// TODO Fix error
	_ = b.matrix.WatchMembers(b.ctx, b.srvname, b.mwatcher)
	_ = b.matrix.WatchValues(b.ctx, b.srvname, b.vwatcher)
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

	for {
		select {
		// Done
		case <-b.ctx.Done():
			return
		// Update endpoint
		case endpoint := <-b.updateMC:
			b.mu.Lock()
			b.endpoints[endpoint.ID] = endpoint
			b.mu.Unlock()
		// Delete endpoint
		case member := <-b.deleteMC:
			b.mu.Lock()
			delete(b.endpoints, member)
			b.mu.Unlock()
		// Update value
		case kv := <-b.updateVC:
			b.mu.Lock()
			b.dict[kv.Key] = kv.Value
			b.mu.Unlock()
		// Delete value
		case key := <-b.deleteVC:
			b.mu.Lock()
			delete(b.dict, key)
			b.mu.Unlock()
		}
	}
}

// Name
func (b *Broker) Name() string {
	return b.srvname
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

// GetValue
func (b *Broker) GetValue(key string) (value []byte, exists bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	value, exists = b.dict[key]
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

type memberWatcher struct {
	b *Broker
}

// OnInit implements Watcher.
func (w *memberWatcher) OnInit(endpoints map[string]Endpoint) {
	w.b.mu.Lock()
	defer w.b.mu.Unlock()
	w.b.endpoints = make(map[string]Endpoint)
	for member, endpoint := range endpoints {
		w.b.endpoints[member] = endpoint
	}
}

// OnUpdate implements Watcher.
func (w *memberWatcher) OnUpdate(member string, endpoint Endpoint) {
	w.b.updateMC <- endpoint
}

// OnDelete implements Watcher.
func (w *memberWatcher) OnDelete(member string) {
	w.b.deleteMC <- member
}

type valueWatcher struct {
	b *Broker
}

// OnInit implements KVWatcher.
func (w *valueWatcher) OnInit(values map[string][]byte) {
	w.b.mu.Lock()
	defer w.b.mu.Unlock()
	w.b.dict = make(map[string][]byte)
	for key, value := range values {
		w.b.dict[key] = value
	}
}

// OnUpdate implements KVWatcher.
func (w *valueWatcher) OnUpdate(key string, value []byte) {
	w.b.updateVC <- kv{key, value}
}

// OnDelete implements KVWatcher.
func (w *valueWatcher) OnDelete(key string) {
	w.b.deleteVC <- key
}
