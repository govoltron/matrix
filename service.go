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
	"sync/atomic"
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

type srvbase struct {
	name string
}

// buildKey
func (sb *srvbase) buildKey(key string) (newkey string) {
	return "/services/" + sb.name + "/" + strings.Trim(key, "/")
}

type ServiceOption func(srv *Service)

type Service struct {
	srvbase
	envs      map[string]string
	endpoints map[string]Endpoint
	updateEC  chan fv
	deleteEC  chan string
	updateMC  chan Endpoint
	deleteMC  chan string
	ewatcher  *fvWatcher
	mwatcher  *memberWatcher
	ctx       context.Context
	cancel    context.CancelFunc
	closed    uint32
	wg        sync.WaitGroup
	mu        sync.RWMutex
	matrix    *Matrix
}

// NewService
func (m *Matrix) NewService(ctx context.Context, srvname string, opts ...ServiceOption) (srv *Service, err error) {
	srv = &Service{
		srvbase: srvbase{
			name: srvname,
		},
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
		setOpt(srv)
	}

	// Context
	srv.ctx, srv.cancel = context.WithCancel(ctx)
	// Background goroutine
	srv.wg.Add(1)
	go srv.background()
	// Watcher
	srv.ewatcher = &fvWatcher{update: srv.updateEC, delete: srv.deleteEC}
	srv.mwatcher = &memberWatcher{update: srv.updateMC, delete: srv.deleteMC}
	// Watch
	if err = srv.matrix.Watch(ctx, srv.buildKey("/env"), srv.ewatcher); err != nil {
		return nil, err
	}
	if err = srv.matrix.Watch(srv.ctx, srv.buildKey("/endpoints"), srv.mwatcher); err != nil {
		return nil, err
	}

	return
}

// background
func (srv *Service) background() {
	defer func() {
		srv.wg.Done()
	}()

	for {
		select {
		// Done
		case <-srv.ctx.Done():
			return
		// Update environment variable
		case fv := <-srv.updateEC:
			srv.mu.Lock()
			srv.envs[fv.Field] = string(fv.Value)
			srv.mu.Unlock()
		// Delete environment variable
		case field := <-srv.deleteEC:
			srv.mu.Lock()
			delete(srv.envs, field)
			srv.mu.Unlock()
		// Update endpoint
		case endpoint := <-srv.updateMC:
			srv.mu.Lock()
			srv.endpoints[endpoint.ID] = endpoint
			srv.mu.Unlock()
		// Delete endpoint
		case member := <-srv.deleteMC:
			srv.mu.Lock()
			delete(srv.endpoints, member)
			srv.mu.Unlock()
		}
	}
}

// Name
func (srv *Service) Name() string {
	return srv.name
}

// Getenv returns the environment variable.
func (srv *Service) Getenv(_ context.Context, key string) (value string) {
	srv.mu.RLock()
	defer srv.mu.RUnlock()
	return srv.envs[key]
}

// Getenv sets the environment variable.
func (srv *Service) Setenv(ctx context.Context, key, value string) (err error) {
	srv.mu.Lock()
	defer func() {
		srv.envs[key] = value
		srv.mu.Unlock()
	}()
	return srv.matrix.Set(ctx, srv.buildKey("/env/"+key), []byte(value), 0)
}

// Delenv deletes the environment variable.
func (srv *Service) Delenv(ctx context.Context, key string) (err error) {
	srv.mu.Lock()
	defer func() {
		delete(srv.envs, key)
		srv.mu.Unlock()
	}()
	return srv.matrix.Delete(ctx, srv.buildKey("/env/"+key))
}

// Endpoints
func (srv *Service) Endpoints() (endpoints []Endpoint) {
	srv.mu.RLock()
	defer srv.mu.RUnlock()
	for _, endpoint := range srv.endpoints {
		endpoints = append(endpoints, endpoint)
	}
	return
}

// Close
func (srv *Service) Close() {
	if !atomic.CompareAndSwapUint32(&srv.closed, 0, 1) {
		return
	}
	srv.cancel()
	srv.wg.Wait()
}
