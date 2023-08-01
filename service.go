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
	"encoding/json"
)

type Endpoint struct {
	Time   int64  `json:"time"`
	ID     string `json:"id"`
	Addr   string `json:"addr"`
	Weight int    `json:"weight"`
}

func (e *Endpoint) Load(b []byte) (err error) {
	return json.Unmarshal(b, e)
}

func (e *Endpoint) Save() (b []byte, err error) {
	return json.Marshal(e)
}

type ServiceKeyParser interface {
	Resolve(name string) (key string)
}

type defaultServiceKeyParser struct {
}

func (kp *defaultServiceKeyParser) Resolve(srvname string) (key string) {
	return srvname
}
