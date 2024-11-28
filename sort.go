// Copyright 2023 Pouya Vedadiyan
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package genql

import (
	"sort"

	"github.com/vedadiyan/genql/compare"
)

func Sort(slice []any, orderBy OrderByDefinition) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
	}()
	if len(orderBy) == 0 {
		return nil
	}
	sort.Slice(slice, func(i, j int) bool {
		rs, err := Compare(slice, i, j, orderBy)
		if err != nil {
			panic(err)
		}
		return rs
	})
	return nil
}

func Compare(slice []any, i int, j int, orderBy OrderByDefinition) (bool, error) {
	if len(orderBy) == 0 {
		return false, nil
	}
	key := orderBy[0].Key
	direction := 1
	if orderBy[0].Value {
		direction = -1
	}
	first, err := ExecReader(slice[i], key)
	if err != nil {
		return false, err
	}
	if first == nil {
		return false, nil
	}
	second, err := ExecReader(slice[j], key)
	if err != nil {
		return false, err
	}
	if second == nil {
		return true, nil
	}
	res := compare.Compare(first, second)
	if res == 0 {
		return Compare(slice, i, j, orderBy[1:])
	}
	return res == direction, nil
}
