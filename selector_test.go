// Copyright 2023 Pouya Vedadiyan
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

package genql

import (
	"fmt"
	"reflect"
	"testing"
)

func TestNewIndex(t *testing.T) {
	tests := []struct {
		name      string
		value     any
		wantType  IndexType
		expectNil bool
	}{
		{
			name:      "Single Integer",
			value:     5,
			wantType:  INDEX,
			expectNil: false,
		},
		{
			name:      "Range Value",
			value:     [2]int{0, 10},
			wantType:  RANGE,
			expectNil: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var indexSelector *IndexSelector
			switch v := tt.value.(type) {
			case int:
				indexSelector = NewIndex(v)
			case [2]int:
				indexSelector = NewIndex(v)
			}

			if tt.expectNil {
				if indexSelector != nil {
					t.Fatal("expected nil IndexSelector, got non-nil")
				}
				return
			}
			if indexSelector == nil {
				t.Fatal("expected non-nil IndexSelector, got nil")
			}
			if indexSelector.selectorType != tt.wantType {
				t.Errorf("expected selectorType to be %v, got %v", tt.wantType, indexSelector.selectorType)
			}

			switch tt.wantType {
			case INDEX:
				if indexSelector.indexSelector != tt.value {
					t.Errorf("expected indexSelector to be %v, got %v", tt.value, indexSelector.indexSelector)
				}
			case RANGE:
				if indexSelector.rangeSelector != tt.value {
					t.Errorf("expected rangeSelector to be %v, got %v", tt.value, indexSelector.rangeSelector)
				}
			}
		})
	}
}
func TestPipeSelector(t *testing.T) {
	tests := []struct {
		name         string
		key          string
		typeSelector string
		wantKeyType  KeyType
	}{
		{
			name:         "Empty Type Returns None",
			key:          "test",
			typeSelector: "",
			wantKeyType:  NONE,
		},
		{
			name:         "String Type",
			key:          "test",
			typeSelector: "string",
			wantKeyType:  STRING,
		},
		{
			name:         "Number Type",
			key:          "test",
			typeSelector: "number",
			wantKeyType:  NUMBER,
		},
		{
			name:         "Unknown Type",
			key:          "test",
			typeSelector: "invalid",
			wantKeyType:  UNKNOWN,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pipeSelector := NewPipe(tt.key, tt.typeSelector)
			if pipeSelector.GetKey() != tt.key {
				t.Errorf("expected key %v, got %v", tt.key, pipeSelector.GetKey())
			}
			if pipeSelector.GetType() != tt.wantKeyType {
				t.Errorf("expected type %v, got %v", tt.wantKeyType, pipeSelector.GetType())
			}
		})
	}
}

func TestReadIndex(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		want      int
		expectErr bool
	}{
		{
			name:      "Valid Positive Integer",
			input:     "42",
			want:      42,
			expectErr: false,
		},
		{
			name:      "Negative Integer",
			input:     "-1",
			want:      0,
			expectErr: true,
		},
		{
			name:      "Invalid Format",
			input:     "abc",
			want:      0,
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ReadIndex(tt.input)
			if tt.expectErr {
				if err == nil {
					t.Fatal("expected an error, got nil")
				}
			} else {
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
				if result != tt.want {
					t.Errorf("expected %d, got %d", tt.want, result)
				}
			}
		})
	}
}

func TestReadRange(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		want      [2]int
		expectErr bool
	}{
		{
			name:      "Valid Range",
			input:     "(0:10)",
			want:      [2]int{0, 10},
			expectErr: false,
		},
		{
			name:      "Begin to End Range",
			input:     "(begin:end)",
			want:      [2]int{-1, -1},
			expectErr: false,
		},
		{
			name:      "Invalid Format",
			input:     "(15)",
			want:      [2]int{},
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ReadRange(tt.input)
			if tt.expectErr {
				if err == nil {
					t.Fatal("expected an error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
			if result.rangeSelector != tt.want {
				t.Errorf("expected %v, got %v", tt.want, result.rangeSelector)
			}
		})
	}
}

func TestParseArray(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		want      interface{}
		expectErr bool
	}{
		{
			name:  "Multiple Indices",
			input: "[1,2,3]",
			want: []*IndexSelector{
				NewIndex(1),
				NewIndex(2),
				NewIndex(3),
			},
			expectErr: false,
		},
		{
			name:  "Range Array",
			input: "[(0:10)]",
			want: []*IndexSelector{
				NewIndex([2]int{0, 10}),
			},
			expectErr: false,
		},
		{
			name:      "Empty Array",
			input:     "[]",
			want:      []*IndexSelector{},
			expectErr: false,
		},
		{
			name:  "Each Keyword",
			input: "[each]",
			want: []*IndexSelector{
				NewIndex(-1),
			},
			expectErr: false,
		},
		{
			name:  "Keep Dimension",
			input: "keep=>[1,2,3]",
			want: KeepDimension{
				NewIndex(1),
				NewIndex(2),
				NewIndex(3),
			},
			expectErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ParseArray(tt.input)
			if tt.expectErr {
				if err == nil {
					t.Fatal("expected an error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
			if !reflect.DeepEqual(result, tt.want) {
				t.Errorf("expected %v, got %v", tt.want, result)
			}
		})
	}
}

func TestParsePipe(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		want      []*PipeSelector
		expectErr bool
	}{
		{
			name:  "Simple Key Value",
			input: "key1|value1",
			want: []*PipeSelector{
				NewPipe("key1", "value1"),
			},
			expectErr: false,
		},
		{
			name:  "Multiple Pipes",
			input: "key1|string,key2|number",
			want: []*PipeSelector{
				NewPipe("key1", "string"),
				NewPipe("key2", "number"),
			},
			expectErr: false,
		},
		{
			name:  "Key Without Type",
			input: "key1",
			want: []*PipeSelector{
				NewPipe("key1", ""),
			},
			expectErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ParsePipe(tt.input)
			if tt.expectErr {
				if err == nil {
					t.Fatal("expected an error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
			if !reflect.DeepEqual(result, tt.want) {
				t.Errorf("expected %v, got %v", tt.want, result)
			}
		})
	}
}

func TestSelectDimension(t *testing.T) {
	tests := []struct {
		name       string
		data       []any
		dimensions []*IndexSelector
		want       interface{}
		expectErr  bool
	}{
		{
			name:       "Empty Dimensions",
			data:       []any{1, 2, 3},
			dimensions: []*IndexSelector{},
			want:       []any{1, 2, 3},
			expectErr:  false,
		},
		{
			name: "Range Selection",
			data: []any{1, 2, 3, 4, 5},
			dimensions: []*IndexSelector{
				NewIndex([2]int{1, 3}),
			},
			want:      []any{2, 3},
			expectErr: false,
		},
		{
			name: "Each Selection",
			data: []any{[]any{1, 2}, []any{3, 4}},
			dimensions: []*IndexSelector{
				NewIndex(-1),
				NewIndex(0),
			},
			want:      []any{1, 3},
			expectErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := SelectDimension(tt.data, tt.dimensions)
			if tt.expectErr {
				if err == nil {
					t.Fatal("expected an error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
			if !reflect.DeepEqual(result, tt.want) {
				t.Errorf("expected %v, got %v", tt.want, result)
			}
		})
	}
}

func TestUnwind(t *testing.T) {
	tests := []struct {
		name  string
		data  []any
		depth int
		want  []any
	}{
		{
			name:  "Zero Depth",
			data:  []any{1, []any{2, 3}, 4},
			depth: 0,
			want:  []any{1, []any{2, 3}, 4},
		},
		{
			name:  "Single Level",
			data:  []any{1, []any{2, 3}, 4},
			depth: 1,
			want:  []any{1, 2, 3, 4},
		},
		{
			name:  "Multiple Levels",
			data:  []any{1, []any{2, []any{3, 4}}, 5},
			depth: 2,
			want:  []any{1, 2, 3, 4, 5},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Unwind(tt.data, tt.depth)
			if !reflect.DeepEqual(result, tt.want) {
				t.Errorf("expected %v, got %v", tt.want, result)
			}
		})
	}
}

func TestSelectObject(t *testing.T) {
	tests := []struct {
		name string
		data map[string]any
		key  string
		want any
	}{
		{
			name: "Existing Key",
			data: map[string]any{
				"key1": "value1",
				"key2": 42,
			},
			key:  "key1",
			want: "value1",
		},
		{
			name: "Non-Existing Key",
			data: map[string]any{
				"key1": "value1",
			},
			key:  "key2",
			want: nil,
		},
		{
			name: "Empty Map",
			data: map[string]any{},
			key:  "key1",
			want: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := SelectObject(tt.data, tt.key)
			if result != tt.want {
				t.Errorf("expected %v, got %v", tt.want, result)
			}
		})
	}
}

func TestMix(t *testing.T) {
	tests := []struct {
		name      string
		input     interface{}
		want      interface{}
		expectErr bool
	}{
		{
			name:      "Array Input",
			input:     []any{1, []any{2, 3}, []any{[]any{4, 5}, 6}, 7},
			want:      []any{1, 2, 3, 4, 5, 6, 7},
			expectErr: false,
		},
		{
			name: "Object Input",
			input: map[string]any{
				"a": map[string]any{
					"b": 1,
					"c": 2,
				},
				"d": 3,
			},
			want: map[string]any{
				"a_b": 1,
				"a_c": 2,
				"d":   3,
			},
			expectErr: false,
		},
		{
			name:      "Invalid Input",
			input:     "invalid",
			want:      nil,
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := Mix(tt.input)
			if tt.expectErr {
				if err == nil {
					t.Fatal("expected an error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
			if !reflect.DeepEqual(result, tt.want) {
				t.Errorf("expected %v, got %v", tt.want, result)
			}
		})
	}
}

func TestDistinct2(t *testing.T) {
	tests := []struct {
		name      string
		input     interface{}
		want      interface{}
		expectErr bool
	}{
		{
			name:      "Array With Duplicates",
			input:     []any{1, 2, 2, 3, 4, 4, 5},
			want:      []any{1, 2, 3, 4, 5},
			expectErr: false,
		},
		{
			name:      "Array Without Duplicates",
			input:     []any{1, 2, 3},
			want:      []any{1, 2, 3},
			expectErr: false,
		},
		{
			name:      "Invalid Input Type",
			input:     "not an array",
			want:      nil,
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := Distinct(tt.input)
			if tt.expectErr {
				if err == nil {
					t.Fatal("expected an error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
			if !reflect.DeepEqual(result, tt.want) {
				t.Errorf("expected %v, got %v", tt.want, result)
			}
		})
	}
}

func TestExecReader(t *testing.T) {
	tests := []struct {
		name      string
		data      interface{}
		selector  string
		want      interface{}
		expectErr bool
	}{
		{
			name: "Simple Key Selection",
			data: map[string]interface{}{
				"name": "John",
				"age":  30,
			},
			selector:  "name",
			want:      "John",
			expectErr: false,
		},
		{
			name: "Empty Selector Returns Original Data",
			data: map[string]interface{}{
				"name": "John",
				"age":  30,
			},
			selector: "",
			want: map[string]interface{}{
				"name": "John",
				"age":  30,
			},
			expectErr: false,
		},
		{
			name: "Chained Selectors",
			data: map[string]interface{}{
				"user": map[string]interface{}{
					"details": map[string]interface{}{
						"name": "John",
					},
				},
			},
			selector:  "user::details::name",
			want:      "John",
			expectErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ExecReader(tt.data, tt.selector)
			if tt.expectErr {
				if err == nil {
					t.Fatal("expected an error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
			if !reflect.DeepEqual(result, tt.want) {
				t.Errorf("expected %v, got %v", tt.want, result)
			}
		})
	}
}

func TestReaderExecutor(t *testing.T) {
	tests := []struct {
		name      string
		data      interface{}
		selectors []interface{}
		want      interface{}
		expectErr bool
	}{
		{
			name: "Top Level Function",
			data: []any{1, 2, 2, 3},
			selectors: []interface{}{
				TopLevelFunctionSelector("distinct"),
			},
			want:      []any{1, 2, 3},
			expectErr: false,
		},
		{
			name:      "Empty Selectors",
			data:      "test data",
			selectors: []interface{}{},
			want:      "test data",
			expectErr: false,
		},
		{
			name: "Invalid Function",
			data: "test",
			selectors: []interface{}{
				TopLevelFunctionSelector("nonexistent"),
			},
			want:      nil,
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ReaderExecutor(tt.data, tt.selectors)
			if tt.expectErr {
				if err == nil {
					t.Fatal("expected an error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
			if !reflect.DeepEqual(result, tt.want) {
				t.Errorf("expected %v, got %v", tt.want, result)
			}
		})
	}
}

func TestReader(t *testing.T) {
	tests := []struct {
		name      string
		data      interface{}
		selectors []interface{}
		want      interface{}
		expectErr bool
	}{
		{
			name: "Key Selector on Map",
			data: map[string]interface{}{
				"key1": "value1",
				"key2": "value2",
			},
			selectors: []interface{}{KeySelector("key1")},
			want:      "value1",
			expectErr: false,
		},
		{
			name:      "Empty Selectors",
			data:      "test",
			selectors: []interface{}{},
			want:      "test",
			expectErr: false,
		},
		{
			name: "Array Index Selector",
			data: []interface{}{1, 2, 3},
			selectors: []interface{}{
				[]*IndexSelector{NewIndex(1)},
			},
			want:      2,
			expectErr: false,
		},
		{
			name: "Pipe Selector",
			data: map[string]interface{}{
				"num": "42",
			},
			selectors: []interface{}{
				[]*PipeSelector{
					NewPipe("num", "number"),
				},
			},
			want: map[string]interface{}{
				"num": float64(42),
			},
			expectErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := Reader(tt.data, tt.selectors)
			if tt.expectErr {
				if err == nil {
					t.Fatal("expected an error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
			if !reflect.DeepEqual(result, tt.want) {
				t.Errorf("expected %v, got %v", tt.want, result)
			}
		})
	}
}

func TestRegisterTopLevelFunction(t *testing.T) {
	tests := []struct {
		name     string
		funcName string
		function func(any) (any, error)
	}{
		{
			name:     "Register New Function",
			funcName: "testFunc",
			function: func(input any) (any, error) {
				return fmt.Sprintf("processed_%v", input), nil
			},
		},
		{
			name:     "Register Function With Empty Name",
			funcName: "",
			function: func(input any) (any, error) {
				return input, nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Reset topLevelFunctions before each test
			topLevelFunctions = nil

			RegisterTopLevelFunction(tt.funcName, tt.function)

			// Verify function was registered
			if _, exists := topLevelFunctions[tt.funcName]; !exists {
				t.Errorf("function %s was not registered", tt.funcName)
			}

			// Verify function works as expected
			result, err := topLevelFunctions[tt.funcName]("test")
			if err != nil {
				t.Errorf("expected no error, got %v", err)
			}
			if tt.funcName == "testFunc" && result != "processed_test" {
				t.Errorf("expected processed_test, got %v", result)
			}
		})
	}
}
