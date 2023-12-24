// Copyright 2012-2023 Pouya Vedadiyan
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
	"bytes"
	"fmt"
)

func DoubleQuotesToBackTick(str string) (string, error) {
	buffer := bytes.NewBufferString("")
	for i := 0; i < len(str); i++ {
		r := rune(str[i])
		switch r {
		case '\'':
			{
				buffer.WriteRune(r)
				i++
				r = '0'
				for ; i < len(str) && r != '\''; i++ {
					r = rune(str[i])
					buffer.WriteRune(r)
					if r == '\\' {
						if i+1 == len(str) {
							return "", fmt.Errorf("index out of range")
						}
						buffer.WriteRune(rune(str[i+1]))
						i++
					}
				}
				i--
			}
		case '`':
			{
				buffer.WriteRune(r)
				i++
				r = '0'
				for ; i < len(str) && r != '`'; i++ {
					r = rune(str[i])
					buffer.WriteRune(r)
				}
				i--
			}
		case '"':
			{
				buffer.WriteRune('`')
				i++
				r = '0'
				for ; i < len(str) && r != '"'; i++ {
					r = rune(str[i])
					if r == '"' {
						buffer.WriteRune('`')
						continue
					}
					if r == '\\' {
						if i+1 == len(str) {
							return "", fmt.Errorf("index out of range")
						}
						next := str[i+1]
						if next == '"' {
							buffer.WriteRune(rune(next))
							i++
							continue
						}
					}
					buffer.WriteRune(r)
				}
				i--
				continue
			}
		default:
			{
				buffer.WriteRune(r)
			}
		}
	}
	return buffer.String(), nil
}
