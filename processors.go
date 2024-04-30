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

func FindArrayIndex(str string) ([][]int, error) {
	var hold *rune
	output := make([][]int, 0)
	stack := make([]int, 0)
	pos := 0
	for i := 0; i < len(str); i++ {
		r := str[i]
		switch r {
		case '\\':
			{
				i++
			}
		case '"':
			{
				if hold == nil {
					r := '"'
					hold = &r
					continue
				}
				if *hold == '"' {
					hold = nil
				}
			}
		case '\'':
			{
				if hold == nil {
					r := '\''
					hold = &r
					continue
				}
				if *hold == '\'' {
					hold = nil
				}
			}
		}
		if hold != nil {
			continue
		}
		switch r {
		case '[':
			{
				index := make([]int, 2)
				index[0] = i
				output = append(output, index)
				stack = append(stack, pos)
				pos++
			}
		case ']':
			{
				if len(stack) == 0 {
					return nil, fmt.Errorf("index out of range")
				}
				index := stack[0]
				output[index][1] = i
				stack = stack[1:]
			}
		}
	}
	return output, nil
}

func FixIdiomaticArray(input string) (string, error) {
	const _TOKEN = "ARRAY"
	indexes, err := FindArrayIndex(input)
	if err != nil {
		panic(err)
	}
	offset := 0
	for _, index := range indexes {
		str := input[:index[0]+offset]
		str += _TOKEN
		str += "("
		str += input[index[0]+offset+1 : index[1]+offset]
		str += ")"
		str += input[index[1]+offset+1:]
		input = str
		offset += len(_TOKEN)
	}
	return input, nil
}
