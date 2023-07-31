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
	"strings"
	"sync"
	"sync/atomic"
)

type BrokerOption func(b *Broker)

// WithBrokerServiceKeyParser
func WithBrokerServiceKeyParser(kparser ServiceKeyParser) BrokerOption {
	return func(b *Broker) { b.kparser = kparser }
}

type Broker struct {
	srvname   string
	envs      map[string]string
	endpoints map[string]Endpoint
	updateEC  chan fv
	deleteEC  chan string
	updateMC  chan Endpoint
	deleteMC  chan string
	ewatcher  *fvWatcher
	mwatcher  *memberWatcher
	kparser   ServiceKeyParser
	ctx       context.Context
	cancel    context.CancelFunc
	closed    uint32
	wg        sync.WaitGroup
	mu        sync.RWMutex
	matrix    *Matrix
}

// NewBroker
func (m *Matrix) NewBroker(ctx context.Context, srvname string, opts ...BrokerOption) (b *Broker) {
	b = &Broker{
		srvname:   srvname,
		envs:      make(map[string]string),
		endpoints: make(map[string]Endpoint),
		updateMC:  make(chan Endpoint, 1),
		deleteMC:  make(chan string, 1),
		updateEC:  make(chan fv, 1),
		deleteEC:  make(chan string, 1),
		matrix:    m,
	}
	// Set options
	for _, setOpt := range opts {
		setOpt(b)
	}
	// Option: kparser
	if b.kparser == nil {
		b.kparser = &defaultServiceKeyParser{}
	}

	// Context
	b.ctx, b.cancel = context.WithCancel(ctx)
	// Watcher
	b.ewatcher = &fvWatcher{update: b.updateEC, delete: b.deleteEC}
	b.mwatcher = &memberWatcher{update: b.updateMC, delete: b.deleteMC}
	// Watch
	// TODO Fix error
	b.matrix.Watch(ctx, b.buildKey("/env"), b.ewatcher)
	b.matrix.Watch(b.ctx, b.buildKey("/endpoints"), b.mwatcher)
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
		// Update environment variable
		case fv := <-b.updateEC:
			b.mu.Lock()
			b.envs[fv.Field] = string(fv.Value)
			b.mu.Unlock()
		// Delete environment variable
		case field := <-b.deleteEC:
			b.mu.Lock()
			delete(b.envs, field)
			b.mu.Unlock()
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
		}
	}
}

// Name
func (b *Broker) Name() string {
	return b.srvname
}

// Getenv returns the environment variable.
func (b *Broker) Getenv(key string) (value string) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.envs[key]
}

// Getenv sets the environment variable.
func (b *Broker) Setenv(ctx context.Context, key, value string) (err error) {
	b.mu.Lock()
	defer func() {
		b.envs[key] = value
		b.mu.Unlock()
	}()
	return b.matrix.Set(ctx, b.buildKey("/env/"+key), []byte(value), 0)
}

// Delenv deletes the environment variable.
func (b *Broker) Delenv(ctx context.Context, key string) (err error) {
	b.mu.Lock()
	defer func() {
		delete(b.envs, key)
		b.mu.Unlock()
	}()
	return b.matrix.Delete(ctx, b.buildKey("/env/"+key))
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

// buildKey
func (b *Broker) buildKey(key string) (newkey string) {
	return "/" + strings.TrimPrefix(b.kparser.Resolve(b.srvname), "/") + "/" + strings.TrimPrefix(key, "/")
}

// Close
func (b *Broker) Close() {
	if !atomic.CompareAndSwapUint32(&b.closed, 0, 1) {
		return
	}
	b.cancel()
	b.wg.Wait()
}
