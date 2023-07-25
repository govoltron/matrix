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
	"time"

	etcd "go.etcd.io/etcd/client/v3"
)

type EtcdKVSOption func(kvs *EtcdKVS)

// WithEtcdDialTimeout
func WithEtcdDialTimeout(timeout time.Duration) EtcdKVSOption {
	return func(kvs *EtcdKVS) {
		if 0 < timeout {
			kvs.config.DialTimeout = timeout
		}
	}
}

// WithEtcdAuthentication
func WithEtcdAuthentication(username, password string) EtcdKVSOption {
	return func(kvs *EtcdKVS) {
		if username != "" {
			kvs.config.Username = username
		}
		if password != "" {
			kvs.config.Password = password
		}
	}
}

// WithEtcdKeySeparator
func WithEtcdKeySeparator(sep string) EtcdKVSOption {
	return func(kvs *EtcdKVS) {
		if sep != "" {
			kvs.separator = sep
		}
	}
}

type EtcdKVS struct {
	config    *etcd.Config
	client    *etcd.Client
	separator string
}

// NewEtcdKVS
func NewEtcdKVS(ctx context.Context, endpoints []string, opts ...EtcdKVSOption) (kvs *EtcdKVS, err error) {
	kvs = &EtcdKVS{
		config: &etcd.Config{
			Context:     ctx,
			Endpoints:   endpoints,
			DialTimeout: 50 * time.Millisecond,
		},
		separator: "/",
	}
	// Set options
	for _, setOpt := range opts {
		setOpt(kvs)
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
		values[string(kv.Key[len(key)+1:])] = kv.Value
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
						member = string(ev.Kv.Key[len(key)+1:])
					)
					if ev.Type == etcd.EventTypePut {
						watcher.OnUpdate(member, ev.Kv.Value)
					} else if ev.Type == etcd.EventTypeDelete {
						watcher.OnDelete(member)
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

// Update implements KVS.
func (kvs *EtcdKVS) Update(ctx context.Context, key, member string, ttl time.Duration, value []byte) (err error) {
	var (
		sec  int64
		opts []etcd.OpOption
	)
	if 0 < ttl {
		sec = int64(ttl / time.Second)
	}
	if sec > 0 {
		if resp, err := kvs.client.Grant(ctx, sec); err != nil {
			return err
		} else {
			opts = append(opts, etcd.WithLease(resp.ID))
		}
	}
	_, err = kvs.client.Put(ctx, key+kvs.separator+member, string(value), opts...)
	return
}

// Delete implements KVS.
func (kvs *EtcdKVS) Delete(ctx context.Context, key, member string) (err error) {
	_, err = kvs.client.Delete(ctx, key+kvs.separator+member)
	return
}

// Close implements KVS.
func (kvs *EtcdKVS) Close(ctx context.Context) (err error) {
	return kvs.client.Close()
}
