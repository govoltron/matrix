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
)

type ConsulStore struct {
}

// NewConsulStore
func NewConsulStore() (kvs *ConsulStore, err error) {
	panic("unimplemented")
}

// Watch implements KvStore.
func (kvs *ConsulStore) Watch(ctx context.Context, key string, watcher Watcher) (err error) {
	panic("unimplemented")
}

// Range implements KvStore.
func (kvs *ConsulStore) Range(ctx context.Context, key string) (values map[string][]byte, err error) {
	panic("unimplemented")
}

// Get implements KvStore.
func (kvs *ConsulStore) Get(ctx context.Context, key string) (value []byte, err error) {
	panic("unimplemented")
}

// Set implements KvStore.
func (kvs *ConsulStore) Set(ctx context.Context, key string, value []byte, ttl time.Duration) (err error) {
	panic("unimplemented")
}

// Delete implements KvStore.
func (kvs *ConsulStore) Delete(ctx context.Context, key string) (err error) {
	panic("unimplemented")
}

// Close implements KvStore.
func (kvs *ConsulStore) Close(ctx context.Context) (err error) {
	panic("unimplemented")
}
