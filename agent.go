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
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	zeroEndpoint Endpoint
	kparser      KeyParser = &defaultKeyParser{}
)

var (
	ErrAgentClosed   = errors.New("agent closed")
	ErrReportTimeout = errors.New("report timeout")
)

type AgentOption func(a *Agent)

// WithDialTimeout
func WithDialTimeout(timeout time.Duration) AgentOption {
	return func(a *Agent) {
		if timeout > 0 {
			a.dialTimeout = timeout
		}
	}
}

// WithKeepalive
func WithKeepalive(timeout, interval time.Duration) AgentOption {
	return func(a *Agent) {
		if timeout > 0 && interval > 0 && timeout > interval {
			a.keepaliveTimeout, a.keepaliveInterval = timeout, interval
		}
	}
}

type Agent struct {
	name              string
	local             Endpoint
	up                chan string
	down              chan string
	remote            map[string]Endpoint
	dialTimeout       time.Duration
	keepaliveTimeout  time.Duration
	keepaliveInterval time.Duration
	wg                sync.WaitGroup
	ctx               context.Context
	cancel            context.CancelFunc
	m                 sync.RWMutex
	closed            uint32
	err               error
}

// NewAgent
func NewAgent(name string, opts ...AgentOption) (a *Agent) {
	a = &Agent{
		name:              name,
		up:                make(chan string, 1),
		down:              make(chan string, 1),
		remote:            make(map[string]Endpoint),
		dialTimeout:       50 * time.Millisecond,
		keepaliveTimeout:  10 * time.Second,
		keepaliveInterval: 5 * time.Second,
	}
	// Set options
	for _, setOption := range opts {
		setOption(a)
	}

	a.ctx, a.cancel = context.WithCancel(context.Background())

	// Sync
	a.wg.Add(1)
	go a.sync()

	return
}

// Name
func (a *Agent) Name() (name string) {
	return a.name
}

// Endpoints
func (a *Agent) Endpoints() (endpoints []Endpoint) {
	endpoints, _ = a.EndpointsWithError()
	return
}

// EndpointsWithError
func (a *Agent) EndpointsWithError() (endpoints []Endpoint, err error) {
	a.m.RLock()
	defer a.m.RUnlock()
	// Read
	if a.err != nil {
		err = a.err
	}
	for _, v := range a.remote {
		endpoints = append(endpoints, v)
	}
	return
}

// Report
func (a *Agent) Report(addr string) {
	if isValidAddr(addr) {
		a.up <- addr
	}
}

// Cancel
func (a *Agent) Cancel(addr string) {
	if isValidAddr(addr) {
		a.down <- addr
	}
}

// Close
func (a *Agent) Close() {
	if atomic.LoadUint32(&a.closed) == 1 {
		return
	}
	a.m.Lock()
	defer a.m.Unlock()
	// Close
	if a.closed == 0 {
		defer atomic.StoreUint32(&a.closed, 1)
		// Release resources
		a.cancel()
		a.wg.Wait()
		close(a.up)
		close(a.down)
		a.local = zeroEndpoint
		a.remote = nil
		a.err = ErrAgentClosed
	}
}

// genrateUniqueID
func (a *Agent) genrateUniqueID(addr string) (uniqueID string) {
	h := sha1.New()
	h.Write([]byte(addr))
	return hex.EncodeToString(h.Sum(nil))[0:12]
}

// sync
func (a *Agent) sync() {
	defer func() {
		a.wg.Done()
	}()

	if a.err = a.sync0(); a.err != nil {
		return
	}

	var (
		prefix   = key(a.name, "/endpoints")
		nchan    = discovery.Watch(a.ctx, prefix)
		timeout  = a.keepaliveTimeout
		interval = a.keepaliveInterval
		ticker   *time.Ticker
		tickerC  <-chan time.Time
	)

	for {
		select {
		// Cancel
		case <-a.ctx.Done():
			if a.local.Addr != "" {
				a.down0(prefix, a.local, time.Now(), timeout)
				a.local = zeroEndpoint
			}
			return
		// Event
		case ev := <-nchan:
			if ev.Op == DiscoveryKeyUpdate {
				a.updateRemote(ev.UniqueID, ev.Value)
			}
			if ev.Op == DiscoveryKeyDelete {
				a.deleteRemote(ev.UniqueID)
			}
		// Ticker
		case now := <-tickerC:
			if a.local.Addr != "" {
				a.setLastError(a.up0(prefix, a.local, now, timeout))
			}
		// Report
		case addr := <-a.up:
			if addr != a.local.Addr {
				backup := a.local
				// Update the new endpoint
				a.local = Endpoint{
					Addr: addr,
					ID:   a.genrateUniqueID(addr),
				}
				a.setLastError(a.up0(prefix, a.local, time.Now(), timeout))
				// Delete the old endpoint
				if backup.Addr != "" {
					a.down0(prefix, a.local, time.Now(), timeout)
				}
				// Start ticker
				ticker = time.NewTicker(interval)
				tickerC = ticker.C
			}
			// Cancel
		case addr := <-a.down:
			if addr == a.local.Addr {
				a.down0(prefix, a.local, time.Now(), timeout)
				a.local = zeroEndpoint
				// Stop ticker
				ticker.Stop()
				tickerC = nil
				ticker = nil
			}
		}
	}
}

// sync0
func (a *Agent) sync0() (err error) {
	var (
		values = make(map[string][]byte)
		prefix = fmt.Sprintf("%s/endpoints", kparser.Resolve(a.Name()))
	)
	// Timeout
	doCtx, cancel := context.WithTimeout(context.Background(), a.dialTimeout)
	defer cancel()
	// Query
	if values, err = discovery.Query(doCtx, prefix); err == nil {
		for uniqueID, v := range values {
			a.updateRemote(uniqueID, v)
		}
	}
	return
}

// up0
func (a *Agent) up0(prefix string, endpoint Endpoint, tm time.Time, timeout time.Duration) (err error) {
	endpoint.Time = tm.Unix()
	// Encode with json
	var buf []byte
	if buf, err = json.Marshal(endpoint); err != nil {
		return
	}
	// Timeout
	doCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	// Update
	return discovery.Update(doCtx, prefix, endpoint.ID, buf, timeout)
}

// down0
func (a *Agent) down0(prefix string, endpoint Endpoint, tm time.Time, timeout time.Duration) (err error) {
	// Timeout
	doCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	// Delete
	return discovery.Delete(doCtx, prefix, endpoint.ID)
}

// setLastError
func (a *Agent) setLastError(err error) {
	if err == context.Canceled {
		a.err = nil
	} else if err == context.DeadlineExceeded {
		a.err = ErrReportTimeout
	}
}

// updateRemote
func (a *Agent) updateRemote(uniqueID string, value []byte) {
	a.m.Lock()
	defer a.m.Unlock()
	// Update
	var endpoint Endpoint
	if err := json.Unmarshal(value, &endpoint); err != nil {
		return
	}
	a.remote[uniqueID] = endpoint
}

// deleteRemote
func (a *Agent) deleteRemote(uniqueID string) {
	a.m.Lock()
	defer a.m.Unlock()
	// Delete
	delete(a.remote, uniqueID)
}

// isValidAddr
func isValidAddr(addr string) bool {
	i := strings.LastIndex(addr, ":")
	if i == -1 {
		return false
	}
	ip, port := net.ParseIP(addr[0:i]), addr[i+1:]
	return ip != nil && port != ""
}

// key
func key(name, suffix string) (k string) {
	if k = kparser.Resolve(name); suffix != "" {
		k += suffix
	}
	return
}

// Key parser
type KeyParser interface {
	Resolve(name string) (key string)
}

type defaultKeyParser struct {
}

// scope convert a service name to its scope key.
func (kp *defaultKeyParser) Resolve(name string) (key string) {
	return fmt.Sprintf("/%s", name)
}

// SetKeyParser
func SetKeyParser(kp KeyParser) {
	if kp == nil {
		panic(errors.New("invalid key parser"))
	}
	kparser = kp
}
