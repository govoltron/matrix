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
	name   string
	ctx    context.Context
	cancel context.CancelFunc
	kvs    KVS
	ENV    *ENV
}

// NewCluster returns a new matrix.
func NewCluster(ctx context.Context, name string, kvs KVS, opts ...MatrixOption) (m *Matrix) {
	m = &Matrix{
		name: name,
		kvs:  kvs,
	}
	// Set options
	for _, setOpt := range opts {
		setOpt(m)
	}

	// Context
	m.ctx, m.cancel = context.WithCancel(ctx)
	// Initialize matrix
	m.init()

	return
}

// init
func (m *Matrix) init() {
	m.initEnv()
}

// Name
func (m *Matrix) Name() string {
	return m.name
}

// Watch
func (m *Matrix) Watch(ctx context.Context, key string, watcher Watcher) (err error) {
	return m.kvs.Watch(ctx, m.buildKey(key), watcher)
}

// Range
func (m *Matrix) Range(ctx context.Context, key string) (values map[string][]byte, err error) {
	return m.kvs.Range(ctx, m.buildKey(key))
}

// Get
func (m *Matrix) Get(ctx context.Context, key string) (value []byte, err error) {
	return m.kvs.Get(ctx, m.buildKey(key))
}

// Set
func (m *Matrix) Set(ctx context.Context, key string, value []byte, ttl time.Duration) (err error) {
	return m.kvs.Set(ctx, m.buildKey(key), value, ttl)
}

// Delete
func (m *Matrix) Delete(ctx context.Context, key string) (err error) {
	return m.kvs.Delete(ctx, m.buildKey(key))
}

// buildKey
func (m *Matrix) buildKey(key string) (newkey string) {
	return "/" + m.name + "/" + strings.TrimPrefix(key, "/")
}

// Close
func (m *Matrix) Close(ctx context.Context) (err error) {
	m.ENV.close(ctx)
	m.cancel()
	return m.kvs.Close(ctx)
}
