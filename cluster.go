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
	OnInit(values map[string][]byte)
	OnUpdate(member string, value []byte)
	OnDelete(member string)
}

// Key Server
type KVS interface {
	Lookup(ctx context.Context, key string) (values map[string][]byte, err error)
	Watch(ctx context.Context, key string, watcher KVWatcher) (err error)
	Update(ctx context.Context, key, member string, ttl time.Duration, values []byte) (err error)
	Delete(ctx context.Context, key, member string) (err error)
	Close(ctx context.Context) (err error)
}

type KeyParser interface {
	Resolve(name string) (key string)
}

type defaultKeyParser struct {
}

func (kp *defaultKeyParser) Resolve(name string) (key string) {
	return name
}

type Watcher interface {
	OnInit(endpoints map[string]Endpoint)
	OnUpdate(member string, endpoint Endpoint)
	OnDelete(member string)
}

type MatrixOption func(m *Matrix)

// WithMatrixKeyParser
func WithMatrixKeyParser(kparser KeyParser) MatrixOption {
	return func(m *Matrix) {
		if kparser != nil {
			m.kparser = kparser
		}
	}
}

type Matrix struct {
	name    string
	ctx     context.Context
	kvs     KVS
	kparser KeyParser
}

// NewCluster returns a new matrix.
func NewCluster(ctx context.Context, kvs KVS, name string, opts ...MatrixOption) (m *Matrix) {
	m = &Matrix{}
	// Set options
	for _, setOpt := range opts {
		setOpt(m)
	}
	m.ctx = ctx
	m.kvs = kvs
	m.name = name
	// Key parser
	if m.kparser == nil {
		m.kparser = &defaultKeyParser{}
	}
	return
}

// Lookup
func (m *Matrix) Lookup(ctx context.Context, name string) (endpoints map[string]Endpoint, err error) {
	values, err := m.kvs.Lookup(ctx, m.buildKey(name))
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

// Watch
func (m *Matrix) Watch(ctx context.Context, name string, watcher Watcher) (err error) {
	return m.kvs.Watch(ctx, m.buildKey(name), &wrapWatcher{watcher})
}

// Update
func (m *Matrix) Update(ctx context.Context, name, member string, ttl time.Duration, endpoint Endpoint) (err error) {
	value, err := endpoint.Save()
	if err != nil {
		return err
	}
	return m.kvs.Update(ctx, m.buildKey(name), member, ttl, value)
}

// Delete
func (m *Matrix) Delete(ctx context.Context, name, member string) (err error) {
	return m.kvs.Delete(ctx, m.buildKey(name), member)
}

// buildKey
func (m *Matrix) buildKey(name string) (key string) {
	key += "/"
	key += m.name
	key += "/"
	key += strings.TrimPrefix(m.kparser.Resolve(name), "/")
	key += "/endpoints"
	return
}

// Close
func (m *Matrix) Close(ctx context.Context) (err error) {
	return m.kvs.Close(ctx)
}

type wrapWatcher struct {
	watcher Watcher
}

// OnInit implements KVWatcher.
func (ww *wrapWatcher) OnInit(values map[string][]byte) {
	var (
		endpoints = make(map[string]Endpoint)
	)
	for member, value := range values {
		var ep Endpoint
		if err1 := ep.Load(value); err1 == nil {
			endpoints[member] = ep
		}
	}
	if len(endpoints) > 0 {
		ww.watcher.OnInit(endpoints)
	}
}

// OnUpdate implements KVWatcher.
func (ww *wrapWatcher) OnUpdate(member string, value []byte) {
	var ep Endpoint
	if err := ep.Load(value); err == nil {
		ww.watcher.OnUpdate(member, ep)
	}
}

// OnDelete implements KVWatcher.
func (ww *wrapWatcher) OnDelete(member string) {
	ww.watcher.OnDelete(member)
}
