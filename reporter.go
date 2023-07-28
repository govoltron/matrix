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
	"sync/atomic"
	"time"
)

type ReporterOption func(r *Reporter)

// WithReporterTimeout
func WithReporterTimeout(timeout time.Duration) ReporterOption {
	return func(r *Reporter) { r.timeout = timeout }
}

type Reporter struct {
	srvname  string
	endpoint Endpoint
	ttl      time.Duration
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
	matrix   *Matrix
}

// NewReporter
func (m *Matrix) NewReporter(ctx context.Context, srvname string, opts ...ReporterOption) (r *Reporter) {
	r = &Reporter{
		srvname:  srvname,
		endpoint: Endpoint{},
		ttl:      0,
		reportT:  nil,
		reportC:  nil,
		cancelC:  make(chan Endpoint, 1),
		preemptC: make(chan func(), 1),
		matrix:   m,
	}
	// Set options
	for _, setOpt := range opts {
		setOpt(r)
	}
	// Option: timeout
	if r.timeout <= 0 {
		r.timeout = 50 * time.Millisecond
	}

	// Context
	r.ctx, r.cancel = context.WithCancel(ctx)
	// Sync
	r.wg.Add(1)
	go r.sync()

	return
}

func (r *Reporter) sync() {
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
			r.register(context.TODO(), r.endpoint, r.ttl)
		// Cancel
		case ep := <-r.cancelC:
			r.unregister(context.TODO(), ep)
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
	return r.srvname
}

// Keepalive
func (r *Reporter) Keepalive(addr string, weight int, ttl time.Duration) {
	if ttl < time.Second {
		panic("TTL must be greater than or equal to 1 second")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	// Preempt
	r.preempt(func() {
		// Cancel old endpoint
		if r.endpoint.Addr != "" {
			r.cancelC <- r.endpoint
		}
		if r.reportT != nil {
			r.reportT.Stop()
			r.reportT = nil
			r.reportC = nil
		}
		// Report new endpoint
		r.ttl = ttl
		if r.ttl <= time.Second {
			ttl = 500 * time.Millisecond
		} else if r.ttl <= 2*time.Second {
			ttl = r.ttl - time.Second
		} else {
			ttl = r.ttl - 2*time.Second
		}
		r.reportT = time.NewTicker(ttl)
		r.reportC = r.reportT.C
		r.endpoint = Endpoint{
			ID:     genrateUniqueID(addr),
			Addr:   addr,
			Weight: weight,
		}
	})
}

func (r *Reporter) Cancel() {
	r.mu.Lock()
	defer r.mu.Unlock()
	// Preempt
	r.preempt(func() {
		// Cancel old endpoint
		if r.endpoint.Addr != "" {
			r.cancelC <- r.endpoint
		}
		if r.reportT != nil {
			r.reportT.Stop()
			r.reportT = nil
			r.reportC = nil
		}
	})
}

// Close
func (r *Reporter) Close() {
	if !atomic.CompareAndSwapUint32(&r.closed, 0, 1) {
		return
	}
	r.cancel()
	r.wg.Wait()
	if r.reportT != nil {
		r.reportT.Stop()
		r.reportT = nil
		r.reportC = nil
	}
	// Cancel endpoint
	if r.endpoint.Addr != "" {
		ctx, cancel := context.WithTimeout(context.Background(), r.timeout)
		defer cancel()
		// Delete endpoint
		r.unregister(ctx, r.endpoint)
	}
}

// register
func (r *Reporter) register(ctx context.Context, endpoint Endpoint, ttl time.Duration) (err error) {
	// Update time
	endpoint.Time = time.Now().Unix()
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	// Update endpoint
	return r.matrix.UpdateMember(ctx, r.srvname, endpoint.ID, ttl, endpoint)
}

// unregister
func (r *Reporter) unregister(ctx context.Context, endpoint Endpoint) (err error) {
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	// Delete endpoint
	return r.matrix.DeleteMember(ctx, r.srvname, endpoint.ID)
}
