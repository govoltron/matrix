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

type Watcher interface {
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
	name    string
	ctx     context.Context
	kvs     KVS
	kparser ServiceKeyParser
}

// NewCluster returns a new matrix.
func NewCluster(ctx context.Context, name string, kvs KVS, opts ...MatrixOption) (m *Matrix) {
	m = &Matrix{
		name: name,
		ctx:  ctx,
		kvs:  kvs,
	}
	// Set options
	for _, setOpt := range opts {
		setOpt(m)
	}
	// Option: kparser
	if m.kparser == nil {
		m.kparser = &defaultServiceKeyParser{}
	}

	return
}

// GetValues
func (m *Matrix) GetValues(ctx context.Context, srvname string) (values map[string][]byte, err error) {
	return m.kvs.Lookup(ctx, m.buildKey(srvname, "/dict"))
}

// WatchValues
func (m *Matrix) WatchValues(ctx context.Context, srvname string, watcher KVWatcher) (err error) {
	return m.kvs.Watch(ctx, m.buildKey(srvname, "/dict"), watcher)
}

// GetValue
func (m *Matrix) GetValue(ctx context.Context, srvname, key string) (value []byte, err error) {
	return m.kvs.Query(ctx, m.buildKey(srvname, "/dict"), key)
}

// SetValue
func (m *Matrix) SetValue(ctx context.Context, srvname, key string, value []byte) (err error) {
	return m.kvs.Update(ctx, m.buildKey(srvname, "/dict"), key, 0, value)
}

// DeleteValue
func (m *Matrix) DeleteValue(ctx context.Context, srvname, key string) (err error) {
	return m.kvs.Delete(ctx, m.buildKey(srvname, "/dict"), key)
}

// GetMembers
func (m *Matrix) GetMembers(ctx context.Context, srvname string) (endpoints map[string]Endpoint, err error) {
	values, err := m.kvs.Lookup(ctx, m.buildKey(srvname, "/endpoints"))
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

// WatchMembers
func (m *Matrix) WatchMembers(ctx context.Context, srvname string, watcher Watcher) (err error) {
	return m.kvs.Watch(ctx, m.buildKey(srvname, "/endpoints"), &wrapWatcher{watcher})
}

// UpdateMember
func (m *Matrix) UpdateMember(ctx context.Context, srvname, member string, ttl time.Duration, endpoint Endpoint) (err error) {
	value, err := endpoint.Save()
	if err != nil {
		return err
	}
	return m.kvs.Update(ctx, m.buildKey(srvname, "/endpoints"), member, ttl, value)
}

// DeleteMember
func (m *Matrix) DeleteMember(ctx context.Context, srvname, member string) (err error) {
	return m.kvs.Delete(ctx, m.buildKey(srvname, "/endpoints"), member)
}

// buildKey
func (m *Matrix) buildKey(srvname, suffix string) (key string) {
	key += "/"
	key += m.name
	key += "/"
	key += strings.TrimPrefix(m.kparser.Resolve(srvname), "/")
	if suffix != "" {
		key += suffix
	}
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
	for field, value := range values {
		var ep Endpoint
		if err1 := ep.Load(value); err1 == nil {
			endpoints[field] = ep
		}
	}
	if len(endpoints) > 0 {
		ww.watcher.OnInit(endpoints)
	}
}

// OnUpdate implements KVWatcher.
func (ww *wrapWatcher) OnUpdate(field string, value []byte) {
	var ep Endpoint
	if err := ep.Load(value); err == nil {
		ww.watcher.OnUpdate(field, ep)
	}
}

// OnDelete implements KVWatcher.
func (ww *wrapWatcher) OnDelete(field string) {
	ww.watcher.OnDelete(field)
}
