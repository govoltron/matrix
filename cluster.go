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
	"fmt"
	"net/url"
	"sync"
	"sync/atomic"
	"time"
)

var (
	baseCtx    context.Context
	baseCancel context.CancelFunc
	discovery  Discovery
	inited     uint32
	m          sync.Mutex
)

type Endpoint struct {
	ID   string `json:"id,omitempty"`
	Addr string `json:"addr,omitempty"`
	Time int64  `json:"time,omitempty"`
}

type DiscoveryKeyOp int32

const (
	DiscoveryKeyUpdate DiscoveryKeyOp = 0
	DiscoveryKeyDelete DiscoveryKeyOp = 1
)

type DiscoveryKeyEvent struct {
	Op       DiscoveryKeyOp
	UniqueID string
	Value    []byte
}

type Discovery interface {
	Query(ctx context.Context, prefix string) (values map[string][]byte, err error)
	Watch(ctx context.Context, prefix string) (nch chan DiscoveryKeyEvent)
	Update(ctx context.Context, prefix, uniqueID string, value []byte, ttl time.Duration) (err error)
	Delete(ctx context.Context, prefix, uniqueID string) (err error)
	Close(ctx context.Context) (err error)
}

// Init
func Init(URL string) (err error) {
	if atomic.LoadUint32(&inited) != 0 {
		return
	}
	m.Lock()
	defer m.Unlock()
	if inited == 0 {
		defer func() {
			if err == nil {
				atomic.StoreUint32(&inited, 1)
			}
		}()
		err = init0(URL)
	}
	return
}

func init0(URL string) (err error) {
	var u *url.URL
	if u, err = url.Parse(URL); err != nil {
		return
	}
	if u.Scheme == "etcd" {
		discovery, err = newEtcdClient(u)
	} else {
		err = fmt.Errorf("discovery engine not supported")
	}
	if err != nil {
		discovery = nil
	} else {
		baseCtx, baseCancel = context.WithCancel(context.Background())
	}
	return
}

// Close
func Close() {
	if atomic.LoadUint32(&inited) == 0 {
		return
	}
	m.Lock()
	defer m.Unlock()
	if inited == 1 {
		defer atomic.StoreUint32(&inited, 0)
		// Release resources
		baseCancel()
		discovery.Close(context.Background())
	}
}
