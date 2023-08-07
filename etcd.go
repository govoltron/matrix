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
	"errors"
	"sync"
	"time"

	etcd "go.etcd.io/etcd/client/v3"
)

type EtcdStoreOption func(kvs *EtcdStore)

// WithEtcdAuthentication
func WithEtcdAuthentication(username, password string) EtcdStoreOption {
	return func(kvs *EtcdStore) {
		kvs.config.Username = username
		kvs.config.Password = password
	}
}

// WithEtcdDialTimeout
func WithEtcdDialTimeout(timeout time.Duration) EtcdStoreOption {
	return func(kvs *EtcdStore) { kvs.config.DialTimeout = timeout }
}

type EtcdStore struct {
	config *etcd.Config
	client *etcd.Client
	leases map[string]map[int64]etcd.LeaseID
	mu     sync.RWMutex
}

// NewEtcdStore
func NewEtcdStore(ctx context.Context, endpoints []string, opts ...EtcdStoreOption) (kvs *EtcdStore, err error) {
	kvs = &EtcdStore{
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

	kvs.client, err = etcd.New(*kvs.config)
	if err != nil {
		return nil, err
	}

	return
}

// Range implements KVS.
func (kvs *EtcdStore) Range(ctx context.Context, key string) (values map[string][]byte, err error) {
	resp, err := kvs.client.Get(ctx, key, etcd.WithPrefix())
	if err != nil {
		return nil, err
	}
	if len(resp.Kvs) > 0 {
		values = make(map[string][]byte)
	}
	for _, kv := range resp.Kvs {
		if len(kv.Key) > len(key) {
			values[string(kv.Key[len(key)+1:])] = kv.Value
		}
	}
	return
}

// Watch implements KVS.
func (kvs *EtcdStore) Watch(ctx context.Context, key string, watcher Watcher) (err error) {
	switch w := watcher.(type) {
	case KeyWatcher:
		return kvs.WatchKey(ctx, key, w)
	case FieldWatcher:
		return kvs.WatchField(ctx, key, w)
	}
	return errors.New("watcher not supported")
}

// WatchKey
func (kvs *EtcdStore) WatchKey(ctx context.Context, key string, watcher KeyWatcher) (err error) {
	value, err := kvs.Get(ctx, key)
	if err != nil {
		return
	}
	watcher.OnInit(key, value)
	// Watch
	wch := kvs.client.Watch(ctx, key)
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
					if ev.Type == etcd.EventTypePut {
						watcher.OnUpdate(key, ev.Kv.Value)
					} else if ev.Type == etcd.EventTypeDelete {
						watcher.OnDelete(key)
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

// WatchField
func (kvs *EtcdStore) WatchField(ctx context.Context, key string, watcher FieldWatcher) (err error) {
	values, err := kvs.Range(ctx, key)
	if err != nil {
		return
	}
	watcher.OnInit(values)
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
					if len(ev.Kv.Key) == len(key) {
						continue
					}
					var (
						field = string(ev.Kv.Key[len(key)+1:])
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

// Get implements KVS.
func (kvs *EtcdStore) Get(ctx context.Context, key string) (value []byte, err error) {
	resp, err := kvs.client.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if len(resp.Kvs) <= 0 {
		return nil, nil
	}
	return resp.Kvs[0].Value, nil
}

// Set implements KVS.
func (kvs *EtcdStore) Set(ctx context.Context, key string, value []byte, ttl time.Duration) (err error) {
	var (
		sec  int64
		opts []etcd.OpOption
	)
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
func (kvs *EtcdStore) Delete(ctx context.Context, key string) (err error) {
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
func (kvs *EtcdStore) getLeaseID(key string, sec int64) (leaseID etcd.LeaseID, ok bool) {
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
func (kvs *EtcdStore) setLeaseID(key string, sec int64, leaseID etcd.LeaseID) {
	kvs.mu.Lock()
	defer kvs.mu.Unlock()
	if _, ok := kvs.leases[key]; !ok {
		kvs.leases[key] = make(map[int64]etcd.LeaseID)
	}
	kvs.leases[key][sec] = leaseID
}

// Close implements KVS.
func (kvs *EtcdStore) Close(ctx context.Context) (err error) {
	return kvs.client.Close()
}
