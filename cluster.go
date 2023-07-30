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
	"encoding/json"
	"strings"
	"sync"
	"time"
)

type Endpoint struct {
	Time   int64  `json:"time"`
	ID     string `json:"id"`
	Addr   string `json:"addr"`
	Weight int    `json:"weight"`
}

func (e *Endpoint) Load(b []byte) (err error) {
	return json.Unmarshal(b, e)
}

func (e *Endpoint) Save() (b []byte, err error) {
	return json.Marshal(e)
}

type KVWatcher interface {
	OnInit(key string, value []byte)
	OnUpdate(key string, value []byte)
	OnDelete(key string)
}

type FVWatcher interface {
	OnInit(values map[string][]byte)
	OnUpdate(field string, value []byte)
	OnDelete(field string)
}

type Watcher interface{}

// Key Server
type KVS interface {
	Watch(ctx context.Context, key string, watcher Watcher) (err error)
	Range(ctx context.Context, key string) (values map[string][]byte, err error)
	Get(ctx context.Context, key string) (value []byte, err error)
	Set(ctx context.Context, key string, value []byte, ttl time.Duration) (err error)
	Delete(ctx context.Context, key string) (err error)
	Close(ctx context.Context) (err error)
}

type ServiceKeyParser interface {
	Resolve(name string) (key string)
}

type defaultServiceKeyParser struct {
}

func (kp *defaultServiceKeyParser) Resolve(srvname string) (key string) {
	return srvname
}

type MatrixOption func(m *Matrix)

type Matrix struct {
	name     string
	ctx      context.Context
	cancel   context.CancelFunc
	kvs      KVS
	envs     map[string]string
	updateEC chan fv
	deleteEC chan string
	ewatcher *fvWatcher
	mu       sync.RWMutex
	wg       sync.WaitGroup
}

// NewCluster returns a new matrix.
func NewCluster(ctx context.Context, name string, kvs KVS, opts ...MatrixOption) (m *Matrix) {
	m = &Matrix{
		name:     name,
		kvs:      kvs,
		envs:     make(map[string]string),
		updateEC: make(chan fv, 1),
		deleteEC: make(chan string, 1),
	}
	// Set options
	for _, setOpt := range opts {
		setOpt(m)
	}

	// Context
	m.ctx, m.cancel = context.WithCancel(ctx)
	// Watcher
	m.ewatcher = &fvWatcher{update: m.updateEC, delete: m.deleteEC}
	// Watch
	// TODO Fix error
	_ = m.Watch(ctx, m.buildNewKey("/env"), m.ewatcher)
	// Background goroutine
	m.wg.Add(1)
	go m.background()

	return
}

// background
func (m *Matrix) background() {
	defer func() {
		m.wg.Done()
	}()

	for {
		select {
		// Done
		case <-m.ctx.Done():
			return
		// Update environment variable
		case fv := <-m.updateEC:
			m.mu.Lock()
			m.envs[fv.Field] = string(fv.Value)
			m.mu.Unlock()
		// Delete environment variable
		case field := <-m.deleteEC:
			m.mu.Lock()
			delete(m.envs, field)
			m.mu.Unlock()
		}
	}
}

// Name
func (m *Matrix) Name() string {
	return m.name
}

// Getenv returns the environment variable.
func (m *Matrix) Getenv(ctx context.Context, key string) (value string) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.envs[key]
}

// Setenv sets the environment variable.
func (m *Matrix) Setenv(ctx context.Context, key, value string) (err error) {
	m.mu.Lock()
	defer func() {
		m.envs[key] = value
		m.mu.Unlock()
	}()
	return m.kvs.Set(ctx, m.buildNewKey("/env/"+key), []byte(value), 0)
}

// Delenv deletes the environment variable.
func (m *Matrix) Delenv(ctx context.Context, key string) (err error) {
	m.mu.Lock()
	defer func() {
		delete(m.envs, key)
		m.mu.Unlock()
	}()
	return m.kvs.Delete(ctx, m.buildNewKey("/env/"+key))
}

// Watch
func (m *Matrix) Watch(ctx context.Context, key string, watcher Watcher) (err error) {
	return m.kvs.Watch(ctx, m.buildNewKey(key), watcher)
}

// Range
func (m *Matrix) Range(ctx context.Context, key string) (values map[string][]byte, err error) {
	return m.kvs.Range(ctx, m.buildNewKey(key))
}

// Get
func (m *Matrix) Get(ctx context.Context, key string) (value []byte, err error) {
	return m.kvs.Get(ctx, m.buildNewKey(key))
}

// Set
func (m *Matrix) Set(ctx context.Context, key string, value []byte, ttl time.Duration) (err error) {
	return m.kvs.Set(ctx, m.buildNewKey(key), value, ttl)
}

// Delete
func (m *Matrix) Delete(ctx context.Context, key string) (err error) {
	return m.kvs.Delete(ctx, m.buildNewKey(key))
}

// buildNewKey
func (m *Matrix) buildNewKey(key string) (newkey string) {
	return "/" + m.name + "/" + strings.TrimPrefix(key, "/")
}

// Close
func (m *Matrix) Close(ctx context.Context) (err error) {
	m.cancel()
	m.wg.Wait()
	return m.kvs.Close(ctx)
}
