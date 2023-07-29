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

const (
	Public string = ""
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
	OnInit(values map[string][]byte)
	OnUpdate(field string, value []byte)
	OnDelete(field string)
}

// Key Server
type KVS interface {
	Lookup(ctx context.Context, key string) (values map[string][]byte, err error)
	Watch(ctx context.Context, key string, watcher KVWatcher) (err error)
	Query(ctx context.Context, key, field string) (value []byte, err error)
	Update(ctx context.Context, key, field string, ttl time.Duration, values []byte) (err error)
	Delete(ctx context.Context, key, field string) (err error)
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

type MemberWatcher interface {
	OnInit(endpoints map[string]Endpoint)
	OnUpdate(member string, endpoint Endpoint)
	OnDelete(member string)
}

type MatrixOption func(m *Matrix)

// WithMatrixServiceKeyParser
func WithMatrixServiceKeyParser(kparser ServiceKeyParser) MatrixOption {
	return func(m *Matrix) { m.kparser = kparser }
}

type Matrix struct {
	name     string
	ctx      context.Context
	cancel   context.CancelFunc
	kvs      KVS
	envs     map[string]string
	updateEC chan kv
	deleteEC chan string
	kwatcher *kvWatcher
	kparser  ServiceKeyParser
	mu       sync.RWMutex
	wg       sync.WaitGroup
}

// NewCluster returns a new matrix.
func NewCluster(ctx context.Context, name string, kvs KVS, opts ...MatrixOption) (m *Matrix) {
	m = &Matrix{
		name:     name,
		kvs:      kvs,
		envs:     make(map[string]string),
		updateEC: make(chan kv, 1),
		deleteEC: make(chan string, 1),
	}
	// Set options
	for _, setOpt := range opts {
		setOpt(m)
	}
	// Option: kparser
	if m.kparser == nil {
		m.kparser = &defaultServiceKeyParser{}
	}
	// Context
	m.ctx, m.cancel = context.WithCancel(ctx)
	// Watcher
	m.kwatcher = &kvWatcher{updateC: m.updateEC, deleteC: m.deleteEC}
	// Watch
	// TODO Fix error
	_ = m.Watchenvs(ctx, Public, m.kwatcher)
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
		case kv := <-m.updateEC:
			m.mu.Lock()
			m.envs[kv.Key] = string(kv.Value)
			m.mu.Unlock()
		// Delete environment variable
		case key := <-m.deleteEC:
			m.mu.Lock()
			delete(m.envs, key)
			m.mu.Unlock()
		}
	}
}

// Name
func (m *Matrix) Name() string {
	return m.name
}

// Watchenvs
func (m *Matrix) Watchenvs(ctx context.Context, srvname string, watcher KVWatcher) (err error) {
	if srvname == Public {
		return m.kvs.Watch(ctx, m.buildKey("env"), m.kwatcher)
	}
	return m.kvs.Watch(ctx, m.buildServiceKey(srvname, "env"), watcher)
}

// Getenv returns the environment variable.
func (m *Matrix) Getenv(ctx context.Context, srvname, key string) (value string, err error) {
	if srvname == Public {
		m.mu.RLock()
		defer m.mu.RUnlock()
		value = m.envs[key]
	} else if v, err1 := m.kvs.Query(ctx, m.buildServiceKey(srvname, "env"), key); err1 == nil && len(v) > 0 {
		value, err = string(v), err1
	}
	return
}

// Setenv sets the environment variable.
func (m *Matrix) Setenv(ctx context.Context, srvname, key, value string) (err error) {
	if srvname == Public {
		m.mu.Lock()
		defer func() {
			m.envs[key] = value
			m.mu.Unlock()
		}()
		return m.kvs.Update(ctx, m.buildKey("env"), key, 0, []byte(value))
	}
	return m.kvs.Update(ctx, m.buildServiceKey(srvname, "env"), key, 0, []byte(value))
}

// Delenv deletes the environment variable.
func (m *Matrix) Delenv(ctx context.Context, srvname, key string) (err error) {
	if srvname == Public {
		m.mu.Lock()
		defer func() {
			delete(m.envs, key)
			m.mu.Unlock()
		}()
		return m.kvs.Delete(ctx, m.buildKey("env"), key)
	}
	return m.kvs.Delete(ctx, m.buildServiceKey(srvname, "env"), key)
}

// WatchMembers
func (m *Matrix) WatchMembers(ctx context.Context, srvname string, watcher MemberWatcher) (err error) {
	return m.kvs.Watch(ctx, m.buildServiceKey(srvname, "endpoints"), &convertWatcher{watcher})
}

// GetMembers
func (m *Matrix) GetMembers(ctx context.Context, srvname string) (endpoints map[string]Endpoint, err error) {
	values, err := m.kvs.Lookup(ctx, m.buildServiceKey(srvname, "endpoints"))
	if err != nil {
		return nil, err
	}
	if len(values) > 0 {
		endpoints = make(map[string]Endpoint)
	}
	for member, value := range values {
		var ep Endpoint
		if err1 := ep.Load(value); err1 == nil {
			endpoints[member] = ep
		}
	}
	return
}

// UpdateMember
func (m *Matrix) UpdateMember(ctx context.Context, srvname, member string, ttl time.Duration, endpoint Endpoint) (err error) {
	value, err := endpoint.Save()
	if err != nil {
		return err
	}
	return m.kvs.Update(ctx, m.buildServiceKey(srvname, "endpoints"), member, ttl, value)
}

// DeleteMember
func (m *Matrix) DeleteMember(ctx context.Context, srvname, member string) (err error) {
	return m.kvs.Delete(ctx, m.buildServiceKey(srvname, "endpoints"), member)
}

// buildKey
func (m *Matrix) buildKey(field string) (key string) {
	return "/" + m.name + "/" + field
}

// buildServiceKey
func (m *Matrix) buildServiceKey(srvname, field string) (key string) {
	key += "/"
	key += m.name
	key += "/"
	key += strings.TrimPrefix(m.kparser.Resolve(srvname), "/")
	key += "/"
	key += field
	return
}

// Close
func (m *Matrix) Close(ctx context.Context) (err error) {
	m.cancel()
	m.wg.Wait()
	return m.kvs.Close(ctx)
}
