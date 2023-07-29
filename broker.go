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
	srvname   string
	envs      map[string]string
	endpoints map[string]Endpoint
	updateEC  chan kv
	deleteEC  chan string
	updateMC  chan Endpoint
	deleteMC  chan string
	kwatcher  *kvWatcher
	mwatcher  *memberWatcher
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
		envs:      make(map[string]string),
		endpoints: make(map[string]Endpoint),
		updateMC:  make(chan Endpoint, 1),
		deleteMC:  make(chan string, 1),
		updateEC:  make(chan kv, 1),
		deleteEC:  make(chan string, 1),
		matrix:    m,
	}
	// Context
	b.ctx, b.cancel = context.WithCancel(ctx)
	// Watcher
	b.kwatcher = &kvWatcher{updateC: b.updateEC, deleteC: b.deleteEC}
	b.mwatcher = &memberWatcher{updateC: b.updateMC, deleteC: b.deleteMC}
	// Watch
	// TODO Fix error
	_ = b.matrix.Watchenvs(b.ctx, b.srvname, b.kwatcher)
	_ = b.matrix.WatchMembers(b.ctx, b.srvname, b.mwatcher)
	// Background goroutine
	b.wg.Add(1)
	go b.background()

	return
}

// background
func (b *Broker) background() {
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
		// Update environment variable
		case kv := <-b.updateEC:
			b.mu.Lock()
			b.envs[kv.Key] = string(kv.Value)
			b.mu.Unlock()
		// Delete environment variable
		case key := <-b.deleteEC:
			b.mu.Lock()
			delete(b.envs, key)
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

// Getenv returns the environment variable.
func (b *Broker) Getenv(key string) (value string) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.envs[key]
}

// Close
func (b *Broker) Close() {
	if !atomic.CompareAndSwapUint32(&b.closed, 0, 1) {
		return
	}
	b.cancel()
	b.wg.Wait()
}
