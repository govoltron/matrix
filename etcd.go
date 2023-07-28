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
	"time"

	etcd "go.etcd.io/etcd/client/v3"
)

type EtcdKVSOption func(kvs *EtcdKVS)

// WithEtcdAuthentication
func WithEtcdAuthentication(username, password string) EtcdKVSOption {
	return func(kvs *EtcdKVS) {
		kvs.config.Username = username
		kvs.config.Password = password
	}
}

// WithEtcdDialTimeout
func WithEtcdDialTimeout(timeout time.Duration) EtcdKVSOption {
	return func(kvs *EtcdKVS) { kvs.config.DialTimeout = timeout }
}

// WithEtcdKeySeparator
func WithEtcdKeySeparator(sep string) EtcdKVSOption {
	return func(kvs *EtcdKVS) { kvs.separator = sep }
}

type EtcdKVS struct {
	config    *etcd.Config
	client    *etcd.Client
	separator string
	leases    map[string]map[int64]etcd.LeaseID
	mu        sync.RWMutex
}

// NewEtcdKVS
func NewEtcdKVS(ctx context.Context, endpoints []string, opts ...EtcdKVSOption) (kvs *EtcdKVS, err error) {
	kvs = &EtcdKVS{
		config: &etcd.Config{
			Context:   ctx,
			Endpoints: endpoints,
		},
		leases: make(map[string]map[int64]etcd.LeaseID),
	}
	// Set options
	for _, setOpt := range opts {
		setOpt(kvs)
	}
	// Option: DialTimeout
	if kvs.config.DialTimeout <= 0 {
		kvs.config.DialTimeout = 50 * time.Millisecond
	}
	// Option: separator
	if kvs.separator == "" {
		kvs.separator = "/"
	}

	kvs.client, err = etcd.New(*kvs.config)
	if err != nil {
		return nil, err
	}

	return
}

// Lookup implements KVS.
func (kvs *EtcdKVS) Lookup(ctx context.Context, key string) (values map[string][]byte, err error) {
	resp, err := kvs.client.Get(ctx, key, etcd.WithPrefix())
	if err != nil {
		return nil, err
	}
	if len(resp.Kvs) > 0 {
		values = make(map[string][]byte)
	}
	for _, kv := range resp.Kvs {
		values[string(kv.Key[len(key)+len(kvs.separator):])] = kv.Value
	}
	return
}

// Watch implements KVS.
func (kvs *EtcdKVS) Watch(ctx context.Context, key string, watcher KVWatcher) (err error) {
	endpoints, err := kvs.Lookup(ctx, key)
	if err != nil {
		return
	}
	watcher.OnInit(endpoints)
	// Watch
	wch := kvs.client.Watch(ctx, key, etcd.WithPrefix())
	// Goroutine for watch
	go func() {
		for {
			select {
			// Cancel
			case <-ctx.Done():
				return
			// Watch
			case resp := <-wch:
				for _, ev := range resp.Events {
					var (
						field = string(ev.Kv.Key[len(key)+len(kvs.separator):])
					)
					if ev.Type == etcd.EventTypePut {
						watcher.OnUpdate(field, ev.Kv.Value)
					} else if ev.Type == etcd.EventTypeDelete {
						watcher.OnDelete(field)
					}
				}
				if resp.Canceled {
					return
				}
			}
		}
	}()

	return
}

// Query implements KVS.
func (kvs *EtcdKVS) Query(ctx context.Context, key, field string) (value []byte, err error) {
	resp, err := kvs.client.Get(ctx, key+kvs.separator+field)
	if err != nil {
		return nil, err
	}
	if len(resp.Kvs) <= 0 {
		return nil, nil
	}
	return resp.Kvs[0].Value, nil
}

// Update implements KVS.
func (kvs *EtcdKVS) Update(ctx context.Context, key, field string, ttl time.Duration, value []byte) (err error) {
	var (
		sec  int64
		opts []etcd.OpOption
	)
	key += kvs.separator + field
	if ttl > 0 {
		sec = int64(ttl / time.Second)
	}
	if sec > 0 {
		var (
			ok      bool
			leaseID etcd.LeaseID
		)
		if leaseID, ok = kvs.getLeaseID(key, sec); !ok {
			if resp, err := kvs.client.Grant(ctx, sec); err != nil {
				return err
			} else {
				leaseID = resp.ID
				kvs.setLeaseID(key, sec, resp.ID)
			}
		} else {
			if _, err := kvs.client.KeepAliveOnce(ctx, leaseID); err != nil {
				return err
			}
		}
		opts = append(opts, etcd.WithLease(leaseID))
	}
	_, err = kvs.client.Put(ctx, key, string(value), opts...)
	return
}

// Delete implements KVS.
func (kvs *EtcdKVS) Delete(ctx context.Context, key, field string) (err error) {
	key += kvs.separator + field
	// Delete key
	_, err = kvs.client.Delete(ctx, key)
	// Delete leases
	if leaseIDs, ok := kvs.leases[key]; ok {
		kvs.mu.Lock()
		defer func() {
			delete(kvs.leases, key)
			kvs.mu.Unlock()
		}()
		for sec, leaseID := range leaseIDs {
			kvs.client.Revoke(ctx, leaseID)
			delete(kvs.leases[key], sec)
		}
	}
	return
}

// getLeaseID
func (kvs *EtcdKVS) getLeaseID(key string, sec int64) (leaseID etcd.LeaseID, ok bool) {
	kvs.mu.RLock()
	defer kvs.mu.RUnlock()
	if leaseIDs, ok1 := kvs.leases[key]; !ok1 {
		return 0, false
	} else {
		leaseID, ok = leaseIDs[sec]
	}
	return
}

// setLeaseID
func (kvs *EtcdKVS) setLeaseID(key string, sec int64, leaseID etcd.LeaseID) {
	kvs.mu.Lock()
	defer kvs.mu.Unlock()
	if _, ok := kvs.leases[key]; !ok {
		kvs.leases[key] = make(map[int64]etcd.LeaseID)
	}
	kvs.leases[key][sec] = leaseID
}

// Close implements KVS.
func (kvs *EtcdKVS) Close(ctx context.Context) (err error) {
	return kvs.client.Close()
}
