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
	"strconv"
	"strings"
	"time"

	// "github.com/allegro/bigcache"
	etcd "go.etcd.io/etcd/client/v3"
)

type etcdclient struct {
	// cache   *bigcache.BigCache
	kv      etcd.KV
	lease   etcd.Lease
	watcher etcd.Watcher
	client  *etcd.Client
}

// newEtcdClient
func newEtcdClient(u *url.URL) (ec *etcdclient, err error) {
	var (
		query  = u.Query()
		config = etcd.Config{}
	)

	// Option: endpoints
	if u.Host != "" {
		config.Endpoints = strings.Split(u.Host, ",")
	} else {
		return nil, fmt.Errorf("invalid hosts for etcd discovery")
	}
	// Option: username/password
	if u.User != nil {
		config.Username = u.User.Username()
		if password, ok := u.User.Password(); ok {
			config.Password = password
		}
	} else {
		if query.Has("username") {
			config.Username = query.Get("username")
		}
		if query.Has("password") {
			config.Password = query.Get("password")
		}
	}
	// Option: dial_timeout
	if query.Has("dial-timeout") {
		timeout, err := strconv.ParseInt(query.Get("dial-timeout"), 10, 64)
		if err == nil && timeout > 0 {
			config.DialTimeout = time.Duration(timeout) * time.Millisecond
		}
	}

	client, err := etcd.New(config)
	if err != nil {
		return nil, err
	}
	// cache, err := bigcache.NewBigCache(bigcache.DefaultConfig(time.Second * 10))
	// if err != nil {
	// 	client.Close()
	// 	client = nil
	// 	return
	// }

	ec = &etcdclient{
		// cache:   cache,
		kv:      etcd.NewKV(client),
		lease:   etcd.NewLease(client),
		watcher: etcd.NewWatcher(client),
		client:  client,
	}

	return
}

// Query
func (ec *etcdclient) Query(ctx context.Context, prefix string) (values map[string][]byte, err error) {
	resp, err := ec.kv.Get(ctx, prefix, etcd.WithPrefix())
	if err != nil {
		return nil, err
	}
	if len(resp.Kvs) > 0 {
		values = make(map[string][]byte)
	}
	for _, kv := range resp.Kvs {
		values[string(kv.Key[len(prefix)+1:])] = kv.Value
	}
	return
}

// Watch
func (ec *etcdclient) Watch(ctx context.Context, prefix string) chan DiscoveryKeyEvent {
	wch := ec.watcher.Watch(ctx, prefix, etcd.WithPrefix())
	nch := make(chan DiscoveryKeyEvent, cap(wch))
	run := func() {
		defer func() {
			close(nch)
		}()
		for {
			select {
			// Cancel
			case <-ctx.Done():
				return
			// Watch
			case resp := <-wch:
				for _, ev := range resp.Events {
					uniqueID := string(ev.Kv.Key[len(prefix)+1:])
					if ev.Type == etcd.EventTypePut {
						nch <- DiscoveryKeyEvent{Op: 0, UniqueID: uniqueID, Value: ev.Kv.Value}
					} else if ev.Type == etcd.EventTypeDelete {
						nch <- DiscoveryKeyEvent{Op: 1, UniqueID: uniqueID, Value: ev.Kv.Value}
					}
				}
				if resp.Canceled {
					return
				}
			}
		}
	}
	// Goroutine
	go run()

	return nch
}

// Update
func (ec *etcdclient) Update(ctx context.Context, prefix, uniqueID string, value []byte, ttl time.Duration) (err error) {
	var (
		sec  int64
		opts []etcd.OpOption
		key  = fmt.Sprintf("%s/%s", prefix, uniqueID)
	)
	if ttl <= 0 {
		sec = 0
	} else {
		sec = int64(ttl / time.Second)
	}
	if sec > 0 {
		if resp, err := ec.lease.Grant(ctx, sec); err != nil {
			return err
		} else {
			opts = append(opts, etcd.WithLease(resp.ID))
		}
	}
	_, err = ec.kv.Put(ctx, key, string(value), opts...)
	return
}

// Delete
func (ec *etcdclient) Delete(ctx context.Context, prefix, uniqueID string) (err error) {
	_, err = ec.kv.Delete(ctx, fmt.Sprintf("%s/%s", prefix, uniqueID))
	return
}

// Close
func (ec *etcdclient) Close(ctx context.Context) (err error) {
	return ec.client.Close()
}
