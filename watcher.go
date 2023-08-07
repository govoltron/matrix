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

type fv struct {
	Field string
	Value []byte
}

type fieldWatcher struct {
	update chan fv
	delete chan string
}

// OnInit implements FieldWatcher.
func (w *fieldWatcher) OnInit(values map[string][]byte) {
	for field, value := range values {
		w.update <- fv{field, value}
	}
}

// OnUpdate implements FieldWatcher.
func (w *fieldWatcher) OnUpdate(field string, value []byte) {
	w.update <- fv{field, value}
}

// OnDelete implements FieldWatcher.
func (w *fieldWatcher) OnDelete(field string) {
	w.delete <- field
}

type endpointWatcher struct {
	update chan Endpoint
	delete chan string
}

// OnInit implements FieldWatcher.
func (w *endpointWatcher) OnInit(values map[string][]byte) {
	for _, value := range values {
		var ep Endpoint
		if err1 := ep.Load(value); err1 == nil {
			w.update <- ep
		}
	}
}

// OnUpdate implements FieldWatcher.
func (w *endpointWatcher) OnUpdate(field string, value []byte) {
	var ep Endpoint
	if err := ep.Load(value); err == nil {
		w.update <- ep
	}
}

// OnDelete implements FieldWatcher.
func (w *endpointWatcher) OnDelete(field string) {
	w.delete <- field
}
