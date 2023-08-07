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
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type KeyWatcher interface {
	OnInit(key string, value []byte)
	OnUpdate(key string, value []byte)
	OnDelete(key string)
}

type FieldWatcher interface {
	OnInit(values map[string][]byte)
	OnUpdate(field string, value []byte)
	OnDelete(field string)
}

type Watcher interface{}

// Key-Value Storage
type KvStore interface {
	Watch(ctx context.Context, key string, watcher Watcher) (err error)
	Range(ctx context.Context, key string) (values map[string][]byte, err error)
	Get(ctx context.Context, key string) (value []byte, err error)
	Set(ctx context.Context, key string, value []byte, ttl time.Duration) (err error)
	Delete(ctx context.Context, key string) (err error)
	Close(ctx context.Context) (err error)
}

type ClusterOption func(m *Cluster)

type Cluster struct {
	name     string
	envs     map[string]string
	updateEC chan fv
	deleteEC chan string
	ewatcher FieldWatcher
	ctx      context.Context
	cancel   context.CancelFunc
	closed   uint32
	wg       sync.WaitGroup
	mu       sync.RWMutex
	kvs      KvStore
}

// NewCluster returns a new cluster.
func NewCluster(ctx context.Context, name string, kvs KvStore, opts ...ClusterOption) (c *Cluster, err error) {
	c = &Cluster{
		name:     name,
		envs:     make(map[string]string),
		updateEC: make(chan fv, 10),
		deleteEC: make(chan string, 10),
		kvs:      kvs,
	}
	// Set options
	for _, setOpt := range opts {
		setOpt(c)
	}

	// Context
	c.ctx, c.cancel = context.WithCancel(ctx)
	// Background goroutine
	c.wg.Add(1)
	go c.background()
	// Watcher
	c.ewatcher = &fieldWatcher{update: c.updateEC, delete: c.deleteEC}
	// Watch
	defer func() {
		if err != nil {
			c.cancel()
		}
	}()
	if err = c.kvs.Watch(c.ctx, c.buildKey("/env"), c.ewatcher); err != nil {
		return nil, err
	}

	return
}

// background
func (c *Cluster) background() {
	defer func() {
		c.wg.Done()
	}()

	for {
		select {
		// Done
		case <-c.ctx.Done():
			return
		// Update environment variable
		case fv := <-c.updateEC:
			c.mu.Lock()
			c.envs[fv.Field] = string(fv.Value)
			c.mu.Unlock()
		// Delete environment variable
		case field := <-c.deleteEC:
			c.mu.Lock()
			delete(c.envs, field)
			c.mu.Unlock()
		}
	}
}

// Name
func (c *Cluster) Name() string {
	return c.name
}

// Getenv returns the environment variable.
func (c *Cluster) Getenv(_ context.Context, key string) (value string) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.envs[key]
}

// Getenv sets the environment variable.
func (c *Cluster) Setenv(ctx context.Context, key, value string) (err error) {
	c.mu.Lock()
	defer func() {
		c.envs[key] = value
		c.mu.Unlock()
	}()
	return c.kvs.Set(ctx, c.buildKey("/env/"+key), []byte(value), 0)
}

// Delenv deletes the environment variable.
func (c *Cluster) Delenv(ctx context.Context, key string) (err error) {
	c.mu.Lock()
	defer func() {
		delete(c.envs, key)
		c.mu.Unlock()
	}()
	return c.kvs.Delete(ctx, c.buildKey("/env/"+key))
}

// Watch
func (c *Cluster) Watch(ctx context.Context, key string, watcher Watcher) (err error) {
	return c.kvs.Watch(ctx, c.buildKey(key), watcher)
}

// Range
func (c *Cluster) Range(ctx context.Context, key string) (values map[string][]byte, err error) {
	return c.kvs.Range(ctx, c.buildKey(key))
}

// Get
func (c *Cluster) Get(ctx context.Context, key string) (value []byte, err error) {
	return c.kvs.Get(ctx, c.buildKey(key))
}

// Set
func (c *Cluster) Set(ctx context.Context, key string, value []byte, ttl time.Duration) (err error) {
	return c.kvs.Set(ctx, c.buildKey(key), value, ttl)
}

// Delete
func (c *Cluster) Delete(ctx context.Context, key string) (err error) {
	return c.kvs.Delete(ctx, c.buildKey(key))
}

// buildKey
func (c *Cluster) buildKey(key string) (newkey string) {
	return "/" + c.name + "/" + strings.Trim(key, "/")
}

// Close
func (c *Cluster) Close(ctx context.Context) (err error) {
	if !atomic.CompareAndSwapUint32(&c.closed, 0, 1) {
		return
	}
	c.cancel()
	c.wg.Wait()
	return c.kvs.Close(ctx)
}
