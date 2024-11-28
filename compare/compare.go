// Copyright 2023 Pouya Vedadiyan
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package compare

import (
	"fmt"
	"strings"
)

func Compare(a, b any) int {
	switch t := a.(type) {
	case int:
		{
			return compare(t, b)
		}
	case int32:
		{
			return compare(t, b)
		}
	case int64:
		{
			return compare(t, b)
		}
	case int16:
		{
			return compare(t, b)
		}
	case int8:
		{
			return compare(t, b)
		}
	case uint:
		{
			return compare(t, b)
		}
	case uint64:
		{
			return compare(t, b)
		}
	case uint32:
		{
			return compare(t, b)
		}
	case uint16:
		{
			return compare(t, b)
		}
	case byte:
		{
			return compare(t, b)
		}
	case float32:
		{
			return compare(t, b)
		}
	case float64:
		{
			return compare(t, b)
		}
	default:
		{
			return strings.Compare(fmt.Sprintf("%v", a), fmt.Sprintf("%v", b))
		}
	}
}

func Cmp[T int | int32 | int64 | int16 | int8 | uint | uint32 | uint64 | uint16 | byte | float32 | float64](a T, b any) int {
	v := As[T](b)
	if a == v {
		return 0
	}
	if a > v {
		return 1
	}
	return -1
}

func As[T int | int32 | int64 | int16 | int8 | uint | uint32 | uint64 | uint16 | byte | float32 | float64](v any) T {
	switch t := v.(type) {
	case int:
		{
			return T(t)
		}
	case int32:
		{
			return T(t)
		}
	case int64:
		{
			return T(t)
		}
	case int16:
		{
			return T(t)
		}
	case int8:
		{
			return T(t)
		}
	case uint:
		{
			return T(t)
		}
	case uint64:
		{
			return T(t)
		}
	case uint32:
		{
			return T(t)
		}
	case uint16:
		{
			return T(t)
		}
	case byte:
		{
			return T(t)
		}
	case float32:
		{
			return T(t)
		}
	case float64:
		{
			return T(t)
		}
	}
	return (*new(T))
}

func compare[T int | int32 | int64 | int16 | int8 | uint | uint32 | uint64 | uint16 | byte | float32 | float64](a T, v any) int {
	switch t := v.(type) {
	case int, int32, int64, int16, int8, uint, uint32, uint64, uint16, byte, float32, float64:
		{
			return Cmp(a, t)
		}
	case string:
		{
			return strings.Compare(fmt.Sprintf("%v", a), t)
		}
	}
	return strings.Compare(fmt.Sprintf("%v", a), fmt.Sprintf("%v", v))
}
