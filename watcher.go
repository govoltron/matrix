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

type kv struct {
	Key   string
	Value []byte
}

type kvWatcher struct {
	updateC chan kv
	deleteC chan string
}

// OnInit implements KVWatcher.
func (w *kvWatcher) OnInit(values map[string][]byte) {
	for key, value := range values {
		w.updateC <- kv{key, value}
	}
}

// OnUpdate implements KVWatcher.
func (w *kvWatcher) OnUpdate(key string, value []byte) {
	w.updateC <- kv{key, value}
}

// OnDelete implements KVWatcher.
func (w *kvWatcher) OnDelete(key string) {
	w.deleteC <- key
}

type memberWatcher struct {
	updateC chan Endpoint
	deleteC chan string
}

// OnInit implements MemberWatcher.
func (w *memberWatcher) OnInit(endpoints map[string]Endpoint) {
	for _, endpoint := range endpoints {
		w.updateC <- endpoint
	}
}

// OnUpdate implements MemberWatcher.
func (w *memberWatcher) OnUpdate(member string, endpoint Endpoint) {
	w.updateC <- endpoint
}

// OnDelete implements MemberWatcher.
func (w *memberWatcher) OnDelete(member string) {
	w.deleteC <- member
}

type convertWatcher struct {
	watcher MemberWatcher
}

// OnInit implements KVWatcher.
func (w *convertWatcher) OnInit(values map[string][]byte) {
	var (
		endpoints = make(map[string]Endpoint)
	)
	for field, value := range values {
		var ep Endpoint
		if err1 := ep.Load(value); err1 == nil {
			endpoints[field] = ep
		}
	}
	if len(endpoints) > 0 {
		w.watcher.OnInit(endpoints)
	}
}

// OnUpdate implements KVWatcher.
func (w *convertWatcher) OnUpdate(field string, value []byte) {
	var ep Endpoint
	if err := ep.Load(value); err == nil {
		w.watcher.OnUpdate(field, ep)
	}
}

// OnDelete implements KVWatcher.
func (w *convertWatcher) OnDelete(field string) {
	w.watcher.OnDelete(field)
}
