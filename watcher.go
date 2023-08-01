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

type fv struct {
	Field string
	Value []byte
}

type fvWatcher struct {
	update chan fv
	delete chan string
}

// OnInit implements FVWatcher.
func (w *fvWatcher) OnInit(values map[string][]byte) {
	for field, value := range values {
		w.update <- fv{field, value}
	}
}

// OnUpdate implements FVWatcher.
func (w *fvWatcher) OnUpdate(field string, value []byte) {
	w.update <- fv{field, value}
}

// OnDelete implements FVWatcher.
func (w *fvWatcher) OnDelete(field string) {
	w.delete <- field
}

type memberWatcher struct {
	update chan Endpoint
	delete chan string
}

// OnInit implements FVWatcher.
func (w *memberWatcher) OnInit(values map[string][]byte) {
	for _, value := range values {
		var ep Endpoint
		if err1 := ep.Load(value); err1 == nil {
			w.update <- ep
		}
	}
}

// OnUpdate implements FVWatcher.
func (w *memberWatcher) OnUpdate(field string, value []byte) {
	var ep Endpoint
	if err := ep.Load(value); err == nil {
		w.update <- ep
	}
}

// OnDelete implements FVWatcher.
func (w *memberWatcher) OnDelete(field string) {
	w.delete <- field
}
