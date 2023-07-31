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
)

type ENV struct {
	envs     map[string]string
	updateEC chan fv
	deleteEC chan string
	ewatcher *fvWatcher
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	mu       sync.RWMutex
	matrix   *Matrix
}

// initEnv
func (m *Matrix) init() {
	e := &ENV{
		envs:     make(map[string]string),
		updateEC: make(chan fv, 1),
		deleteEC: make(chan string, 1),
		matrix:   m,
	}
	// Context
	e.ctx, e.cancel = context.WithCancel(m.ctx)
	// Watcher
	e.ewatcher = &fvWatcher{update: e.updateEC, delete: e.deleteEC}
	// Watch
	// TODO Fix error
	_ = m.Watch(m.ctx, "/env", e.ewatcher)
	// Background goroutine
	e.wg.Add(1)
	go e.background()

	m.ENV = e
}

// background
func (e *ENV) background() {
	defer func() {
		e.wg.Done()
	}()

	for {
		select {
		// Done
		case <-e.ctx.Done():
			return
		// Update environment variable
		case fv := <-e.updateEC:
			e.mu.Lock()
			e.envs[fv.Field] = string(fv.Value)
			e.mu.Unlock()
		// Delete environment variable
		case field := <-e.deleteEC:
			e.mu.Lock()
			delete(e.envs, field)
			e.mu.Unlock()
		}
	}
}

// Getenv returns the environment variable.
func (e *ENV) Get(ctx context.Context, key string) (value string) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.envs[key]
}

// Setenv sets the environment variable.
func (e *ENV) Set(ctx context.Context, key, value string) (err error) {
	e.mu.Lock()
	defer func() {
		e.envs[key] = value
		e.mu.Unlock()
	}()
	return e.matrix.Set(ctx, "/env/"+key, []byte(value), 0)
}

// Delenv deletes the environment variable.
func (e *ENV) Delete(ctx context.Context, key string) (err error) {
	e.mu.Lock()
	defer func() {
		delete(e.envs, key)
		e.mu.Unlock()
	}()
	return e.matrix.Delete(ctx, "/env/"+key)
}

// close
func (e *ENV) close(ctx context.Context) {
	e.cancel()
	e.wg.Wait()
}
