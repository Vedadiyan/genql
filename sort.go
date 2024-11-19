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
	"fmt"
	"sort"
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
	direction := orderBy[0].Value
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
	switch first.(type) {
	case int, int16, int32, int64, int8, float32, float64, byte:
		{
			firstValue, ok := first.(float64)
			if !ok {
				return false, INVALID_TYPE.Extend(fmt.Sprintf("failed to compare values. expected number but found %T", first))
			}
			secondValue, ok := second.(float64)
			if !ok {
				return false, INVALID_TYPE.Extend(fmt.Sprintf("failed to compare values. expected number but found %T", first))
			}
			if firstValue == secondValue {
				return Compare(slice, i, j, orderBy[1:])
			}
			return firstValue > secondValue != direction, nil
		}
	case string:
		{
			firstValue, ok := first.(string)
			if !ok {
				return false, INVALID_TYPE.Extend(fmt.Sprintf("failed to compare values. expected string but found %T", first))
			}
			secondValue, ok := second.(string)
			if !ok {
				return false, INVALID_TYPE.Extend(fmt.Sprintf("failed to compare values. expected string but found %T", first))
			}
			if firstValue == secondValue {
				return Compare(slice, i, j, orderBy[1:])
			}
			return firstValue > secondValue != direction, nil
		}
	case bool:
		{
			firstValueRaw, ok := first.(bool)
			if !ok {
				return false, INVALID_TYPE.Extend(fmt.Sprintf("failed to compare values. expected boolean but found %T", first))
			}
			secondValueRaw, ok := second.(bool)
			if !ok {
				return false, INVALID_TYPE.Extend(fmt.Sprintf("failed to compare values. expected boolean but found %T", first))
			}
			if first == second {
				return Compare(slice, i, j, orderBy[1:])
			}
			firstValue := 0
			secondValue := 0
			if firstValueRaw {
				firstValue = 1
			}
			if secondValueRaw {
				secondValue = 1
			}
			return firstValue > secondValue != direction, nil
		}
	}
	return false, nil
}
