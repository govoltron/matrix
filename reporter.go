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
	"time"
)

type ReporterOption func(r *Reporter)

// WithReporterTimeout
func WithReporterTimeout(timeout time.Duration) ReporterOption {
	return func(r *Reporter) { r.timeout = timeout }
}

type Reporter struct {
	srvbase
	endpoint Endpoint
	ttl      int64
	cancelC  chan Endpoint
	preemptC chan func()
	reportT  *time.Ticker
	reportC  <-chan time.Time
	ctx      context.Context
	cancel   context.CancelFunc
	timeout  time.Duration
	closed   uint32
	wg       sync.WaitGroup
	mu       sync.RWMutex
	cluster  *Cluster
}

// NewReporter
func (c *Cluster) NewReporter(ctx context.Context, srvname string, opts ...ReporterOption) (r *Reporter) {
	r = &Reporter{
		srvbase: srvbase{
			name: srvname,
		},
		endpoint: Endpoint{},
		ttl:      0,
		reportT:  nil,
		reportC:  nil,
		cancelC:  make(chan Endpoint, 1),
		preemptC: make(chan func(), 1),
		cluster:  c,
	}
	// Set options
	for _, setOpt := range opts {
		setOpt(r)
	}
	// Option: timeout
	if r.timeout <= 0 {
		r.timeout = 100 * time.Millisecond
	}

	// Context
	r.ctx, r.cancel = context.WithCancel(ctx)
	// Background goroutine
	r.wg.Add(1)
	go r.background()

	return
}

// background
func (r *Reporter) background() {
	defer func() {
		r.wg.Done()
	}()

	for {
		select {
		// Done
		case <-r.ctx.Done():
			return
		// Report
		case <-r.reportC:
			r.register(r.ctx, r.endpoint, r.ttl)
		// Cancel
		case ep := <-r.cancelC:
			r.unregister(r.ctx, ep)
		// Preempt
		case preempt := <-r.preemptC:
			preempt()
		}
	}
}

// preempt
func (r *Reporter) preempt(fun func()) {
	r.preemptC <- fun
}

// Name
func (r *Reporter) Name() string {
	return r.name
}

// Keepalive
func (r *Reporter) Keepalive(addr string, weight int, ttl int64) {
	if ttl <= 0 {
		panic("TTL must be greater than or equal to 1 second")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	// Preempt
	r.preempt(func() {
		// Cancel old endpoint
		r.tryCancel()
		// Report new endpoint
		tick := time.Duration(ttl) * time.Second
		if ttl <= 1 {
			tick = 500 * time.Millisecond
		} else if ttl <= 2 {
			tick -= time.Second
		} else {
			tick -= 2 * time.Second
		}
		r.endpoint = Endpoint{
			ID:     genrateUniqueID(addr),
			Addr:   addr,
			Weight: weight,
		}
		r.ttl = ttl
		r.reportT = time.NewTicker(tick)
		r.reportC = r.reportT.C
		// First report
		r.register(r.ctx, r.endpoint, r.ttl)
	})
}

// Cancel
func (r *Reporter) Cancel() {
	r.mu.Lock()
	defer r.mu.Unlock()
	// Preempt
	r.preempt(func() {
		// Cancel old endpoint
		r.tryCancel()
	})
}

// tryCancel
func (r *Reporter) tryCancel() {
	if r.endpoint.Addr != "" {
		r.cancelC <- r.endpoint
	}
	if r.reportT != nil {
		r.reportT.Stop()
		r.reportT = nil
		r.reportC = nil
	}
}

// register
func (r *Reporter) register(ctx context.Context, endpoint Endpoint, ttl int64) (err error) {
	value, err := endpoint.Save()
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	// Update endpoint
	return r.cluster.Set(ctx, r.buildKey("/endpoints/"+endpoint.ID), value, ttl)
}

// unregister
func (r *Reporter) unregister(ctx context.Context, endpoint Endpoint) (err error) {
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	// Delete endpoint
	return r.cluster.Delete(ctx, r.buildKey("/endpoints"+endpoint.ID))
}

// Close
func (r *Reporter) Close(ctx context.Context) {
	if !atomic.CompareAndSwapUint32(&r.closed, 0, 1) {
		return
	}
	r.cancel()
	r.wg.Wait()
	// Cancel endpoint
	if r.endpoint.Addr != "" {
		r.unregister(ctx, r.endpoint)
	}
	if r.reportT != nil {
		r.reportT.Stop()
		r.reportT = nil
		r.reportC = nil
	}
}
