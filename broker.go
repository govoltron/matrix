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
	"context"
	"sync"
	"sync/atomic"
)

type BrokerWatcher interface {
	OnSetenv(key, value string)
	OnDelenv(key string)
	OnUpdateEndpoint(endpoint Endpoint)
	OnDeleteEndpoint(id string)
}

type BrokerOption func(b *Broker)

type Broker struct {
	srvbase
	envs      map[string]string
	endpoints map[string]Endpoint
	updateEC  chan fv
	deleteEC  chan string
	updateMC  chan Endpoint
	deleteMC  chan string
	ewatcher  FieldWatcher
	mwatcher  FieldWatcher
	bwatcher  BrokerWatcher
	ctx       context.Context
	cancel    context.CancelFunc
	closed    uint32
	wg        sync.WaitGroup
	mu        sync.RWMutex
	cluster   *Cluster
}

// NewBroker
func (c *Cluster) NewBroker(ctx context.Context, srvname string, opts ...BrokerOption) (b *Broker, err error) {
	b = &Broker{
		srvbase: srvbase{
			name: srvname,
		},
		envs:      make(map[string]string),
		endpoints: make(map[string]Endpoint),
		updateMC:  make(chan Endpoint, 10),
		deleteMC:  make(chan string, 10),
		updateEC:  make(chan fv, 10),
		deleteEC:  make(chan string, 10),
		cluster:   c,
	}
	// Set options
	for _, setOpt := range opts {
		setOpt(b)
	}

	// Context
	b.ctx, b.cancel = context.WithCancel(ctx)
	// Background goroutine
	b.wg.Add(1)
	go b.background()
	// Watcher
	b.ewatcher = &fieldWatcher{update: b.updateEC, delete: b.deleteEC}
	b.mwatcher = &endpointWatcher{update: b.updateMC, delete: b.deleteMC}
	// Watch
	defer func() {
		if err != nil {
			b.cancel()
		}
	}()
	if err = b.cluster.Watch(ctx, b.buildKey("/env"), b.ewatcher); err != nil {
		return nil, err
	}
	if err = b.cluster.Watch(b.ctx, b.buildKey("/endpoints"), b.mwatcher); err != nil {
		return nil, err
	}

	return
}

// background
func (b *Broker) background() {
	defer func() {
		b.wg.Done()
	}()

	for {
		var (
			bwatcher BrokerWatcher
		)
		select {
		// Done
		case <-b.ctx.Done():
			return
		// Update environment variable
		case fv := <-b.updateEC:
			value := string(fv.Value)
			b.mu.Lock()
			bwatcher = b.bwatcher
			b.envs[fv.Field] = value
			b.mu.Unlock()
			if bwatcher != nil {
				bwatcher.OnSetenv(fv.Field, value)
			}
		// Delete environment variable
		case field := <-b.deleteEC:
			b.mu.Lock()
			bwatcher = b.bwatcher
			delete(b.envs, field)
			b.mu.Unlock()
			if bwatcher != nil {
				bwatcher.OnDelenv(field)
			}
		// Update endpoint
		case endpoint := <-b.updateMC:
			b.mu.Lock()
			bwatcher = b.bwatcher
			b.endpoints[endpoint.ID] = endpoint
			b.mu.Unlock()
			if bwatcher != nil {
				bwatcher.OnUpdateEndpoint(endpoint)
			}
		// Delete endpoint
		case id := <-b.deleteMC:
			b.mu.Lock()
			bwatcher = b.bwatcher
			delete(b.endpoints, id)
			b.mu.Unlock()
			if bwatcher != nil {
				bwatcher.OnDeleteEndpoint(id)
			}
		}
	}
}

// Name
func (b *Broker) Name() string {
	return b.name
}

// Getenv returns the environment variable.
func (b *Broker) Getenv(_ context.Context, key string) (value string) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.envs[key]
}

// Endpoints
func (b *Broker) Endpoints() (endpoints []Endpoint) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	for _, endpoint := range b.endpoints {
		endpoints = append(endpoints, endpoint)
	}
	return
}

// Watch
func (b *Broker) Watch(watcher BrokerWatcher) {
	b.mu.Lock()
	defer b.mu.Unlock()
	// Set watcher
	b.bwatcher = watcher
}

// Close
func (b *Broker) Close() {
	if !atomic.CompareAndSwapUint32(&b.closed, 0, 1) {
		return
	}
	b.cancel()
	b.wg.Wait()
}
