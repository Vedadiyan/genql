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
	"math"
	"reflect"
	"sync"
	"testing"
)

func TestSumFunc(t *testing.T) {
	tests := []struct {
		name            string
		query           *Query
		current         Map
		functionOptions *FunctionOptions
		args            []any
		want            any
		expectErr       bool
	}{
		{
			name:            "Calculates Sum Correctly",
			query:           &Query{},
			current:         Map{},
			functionOptions: &FunctionOptions{},
			args:            []any{[]any{1.0, 2.0, 3.0}},
			want:            6.0,
			expectErr:       false,
		},
		{
			name:            "Invalid Input",
			query:           &Query{},
			current:         Map{},
			functionOptions: &FunctionOptions{},
			args:            []any{"not a list"},
			want:            nil,
			expectErr:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := SumFunc(tt.query, tt.current, tt.functionOptions, tt.args)
			if tt.expectErr {
				if err == nil {
					t.Fatal("expected an error, got none")
				}
			} else {
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
				if result != tt.want {
					t.Errorf("expected %v, got %v", tt.want, result)
				}
			}
		})
	}
}

func TestAvgFunc(t *testing.T) {
	tests := []struct {
		name            string
		query           *Query
		current         Map
		functionOptions *FunctionOptions
		args            []any
		want            any
		expectErr       bool
	}{
		{
			name:            "Computes Average Correctly",
			query:           &Query{},
			current:         Map{},
			functionOptions: &FunctionOptions{},
			args:            []any{[]any{1.0, 2.0, 3.0, 4.0, 5.0}},
			want:            3.0,
			expectErr:       false,
		},
		{
			name:            "Handle Non-Numeric Values",
			query:           &Query{},
			current:         Map{},
			functionOptions: &FunctionOptions{},
			args:            []any{[]any{1, "two", 3}},
			want:            nil,
			expectErr:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := AvgFunc(tt.query, tt.current, tt.functionOptions, tt.args)
			if tt.expectErr {
				if err == nil {
					t.Fatal("expected an error, got none")
				}
			} else {
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
				if result != tt.want {
					t.Errorf("expected %v, got %v", tt.want, result)
				}
			}
		})
	}
}

func TestMinFunc(t *testing.T) {
	tests := []struct {
		name            string
		query           *Query
		current         Map
		functionOptions *FunctionOptions
		args            []any
		want            any
		expectErr       bool
	}{
		{
			name:            "Returns Minimum Value",
			query:           &Query{},
			current:         Map{},
			functionOptions: &FunctionOptions{},
			args:            []any{[]any{3.5, 2.1, 4.8, 1.9}},
			want:            1.9,
			expectErr:       false,
		},
		{
			name:            "Handles Mixed Numeric Types",
			query:           &Query{},
			current:         Map{},
			functionOptions: &FunctionOptions{},
			args:            []any{[]any{3.5, 2, 4.8, 1.9}},
			want:            1.9,
			expectErr:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := MinFunc(tt.query, tt.current, tt.functionOptions, tt.args)
			if tt.expectErr {
				if err == nil {
					t.Fatal("expected an error, got none")
				}
			} else {
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
				if result != tt.want {
					t.Errorf("expected %v, got %v", tt.want, result)
				}
			}
		})
	}
}

func TestMaxFunc(t *testing.T) {
	tests := []struct {
		name            string
		query           *Query
		current         Map
		functionOptions *FunctionOptions
		args            []any
		want            any
		expectErr       bool
	}{
		{
			name:            "Returns Maximum Value",
			query:           &Query{},
			current:         Map{},
			functionOptions: &FunctionOptions{},
			args:            []any{[]any{1.0, 2.5, 3.7, 0.9}},
			want:            3.7,
			expectErr:       false,
		},
		{
			name:            "Handles Empty Slice",
			query:           &Query{},
			current:         Map{},
			functionOptions: &FunctionOptions{},
			args:            []any{[]any{}},
			want:            -math.MaxFloat64,
			expectErr:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := MaxFunc(tt.query, tt.current, tt.functionOptions, tt.args)
			if tt.expectErr {
				if err == nil {
					t.Fatal("expected an error, got none")
				}
			} else {
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
				if result != tt.want {
					t.Errorf("expected %v, got %v", tt.want, result)
				}
			}
		})
	}
}

func TestCountFunc(t *testing.T) {
	tests := []struct {
		name            string
		query           *Query
		current         Map
		functionOptions *FunctionOptions
		args            []any
		want            any
		expectErr       bool
	}{
		{
			name:            "With Empty Args",
			query:           &Query{},
			current:         Map{},
			functionOptions: &FunctionOptions{},
			args:            []any{[]any{1.0, 2.0, 3.0}},
			want:            3,
			expectErr:       false,
		},
		{
			name:            "Empty Processed List",
			query:           &Query{},
			current:         Map{"*": []any{1.0, 2.0, 3.0}},
			functionOptions: &FunctionOptions{},
			args:            []any{},
			want:            3,
			expectErr:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := CountFunc(tt.query, tt.current, tt.functionOptions, tt.args)
			if tt.expectErr {
				if err == nil {
					t.Fatal("expected an error, got none")
				}
			} else {
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
				if result != tt.want {
					t.Errorf("expected %v, got %v", tt.want, result)
				}
			}
		})
	}
}

func TestConcatFunc(t *testing.T) {
	tests := []struct {
		name            string
		query           *Query
		current         Map
		functionOptions *FunctionOptions
		args            []any
		want            any
		expectErr       bool
	}{
		{
			name:            "Strings and Numbers",
			query:           &Query{},
			current:         Map{},
			functionOptions: &FunctionOptions{},
			args:            []any{"Hello", 123, "World", 456},
			want:            "Hello123World456",
			expectErr:       false,
		},
		{
			name:            "With Nil Values",
			query:           &Query{},
			current:         Map{},
			functionOptions: &FunctionOptions{},
			args:            []any{"Hello", nil, "World", nil},
			want:            "Hello<nil>World<nil>",
			expectErr:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ConcatFunc(tt.query, tt.current, tt.functionOptions, tt.args)
			if tt.expectErr {
				if err == nil {
					t.Fatal("expected an error, got none")
				}
			} else {
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
				if result != tt.want {
					t.Errorf("expected %v, got %v", tt.want, result)
				}
			}
		})
	}
}

func TestFirstFunc(t *testing.T) {
	tests := []struct {
		name            string
		query           *Query
		current         Map
		functionOptions *FunctionOptions
		args            []any
		want            any
		expectErr       bool
	}{
		{
			name:            "Returns First Element Of Non-Empty Slice",
			query:           &Query{},
			current:         Map{},
			functionOptions: &FunctionOptions{},
			args:            []any{[]any{1, 2, 3}},
			want:            1,
			expectErr:       false,
		},
		{
			name:            "Handles Nil As First Element",
			query:           &Query{},
			current:         Map{},
			functionOptions: &FunctionOptions{},
			args:            []any{nil},
			want:            nil,
			expectErr:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := FirstFunc(tt.query, tt.current, tt.functionOptions, tt.args)
			if tt.expectErr {
				if err == nil {
					t.Fatal("expected an error, got none")
				}
			} else {
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
				if result != tt.want {
					t.Errorf("expected %v, got %v", tt.want, result)
				}
			}
		})
	}
}

func TestLastFunc(t *testing.T) {
	tests := []struct {
		name            string
		query           *Query
		current         Map
		functionOptions *FunctionOptions
		args            []any
		want            any
		expectErr       bool
	}{
		{
			name:            "Returns Last Element Of Non-Empty Slice",
			query:           &Query{},
			current:         Map{},
			functionOptions: &FunctionOptions{},
			args:            []any{[]any{1, 2, 3, 4, 5}},
			want:            5,
			expectErr:       false,
		},
		{
			name:            "Handles Single Element Slice",
			query:           &Query{},
			current:         Map{},
			functionOptions: &FunctionOptions{},
			args:            []any{[]any{42}},
			want:            42,
			expectErr:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := LastFunc(tt.query, tt.current, tt.functionOptions, tt.args)
			if tt.expectErr {
				if err == nil {
					t.Fatal("expected an error, got none")
				}
			} else {
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
				if result != tt.want {
					t.Errorf("expected %v, got %v", tt.want, result)
				}
			}
		})
	}
}

func TestElementAtFunc(t *testing.T) {
	tests := []struct {
		name            string
		query           *Query
		current         Map
		functionOptions *FunctionOptions
		args            []any
		want            any
		expectErr       bool
	}{
		{
			name:            "Returns Correct Element",
			query:           &Query{},
			current:         Map{},
			functionOptions: &FunctionOptions{},
			args:            []any{[]any{"a", "b", "c"}, 1.0},
			want:            "b",
			expectErr:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ElementAtFunc(tt.query, tt.current, tt.functionOptions, tt.args)
			if tt.expectErr {
				if err == nil {
					t.Fatal("expected an error, got none")
				}
			} else {
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
				if result != tt.want {
					t.Errorf("expected %v, got %v", tt.want, result)
				}
			}
		})
	}
}

func TestDefaultKeyFunc(t *testing.T) {
	tests := []struct {
		name            string
		query           *Query
		current         Map
		functionOptions *FunctionOptions
		args            []any
		want            any
		expectErr       bool
	}{
		{
			name:            "Returns Value For Single Key-Value Pair",
			query:           &Query{},
			current:         Map{},
			functionOptions: &FunctionOptions{},
			args:            []any{Map{"key": "value"}},
			want:            "value",
			expectErr:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := DefaultKeyFunc(tt.query, tt.current, tt.functionOptions, tt.args)
			if tt.expectErr {
				if err == nil {
					t.Fatal("expected an error, got none")
				}
			} else {
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
				if result != tt.want {
					t.Errorf("expected %v, got %v", tt.want, result)
				}
			}
		})
	}
}

func TestChangeTypeFunc(t *testing.T) {
	tests := []struct {
		name            string
		query           *Query
		current         Map
		functionOptions *FunctionOptions
		args            []any
		want            any
		expectErr       bool
	}{
		{
			name:            "Converts To Array",
			query:           &Query{},
			current:         Map{},
			functionOptions: &FunctionOptions{},
			args:            []any{42, "array"},
			want:            []any{42},
			expectErr:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ChangeTypeFunc(tt.query, tt.current, tt.functionOptions, tt.args)
			if tt.expectErr {
				if err == nil {
					t.Fatal("expected an error, got none")
				}
			} else {
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
				if !reflect.DeepEqual(result, tt.want) {
					t.Errorf("expected %v, got %v", tt.want, result)
				}
			}
		})
	}
}

func TestUnwindFunc(t *testing.T) {
	tests := []struct {
		name            string
		query           *Query
		current         Map
		functionOptions *FunctionOptions
		args            []any
		want            any
		expectErr       bool
	}{
		{
			name:            "Unwinds Nested List",
			query:           &Query{},
			current:         Map{},
			functionOptions: &FunctionOptions{},
			args:            []any{[]any{[]any{1, 2}, 3, []any{4, 5}}},
			want:            []any{1, 2, 3, 4, 5},
			expectErr:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := UnwindFunc(tt.query, tt.current, tt.functionOptions, tt.args)
			if tt.expectErr {
				if err == nil {
					t.Fatal("expected an error, got none")
				}
			} else {
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
				if !reflect.DeepEqual(result, tt.want) {
					t.Errorf("expected %v, got %v", tt.want, result)
				}
			}
		})
	}
}

func TestIfFunc(t *testing.T) {
	tests := []struct {
		name            string
		query           *Query
		current         Map
		functionOptions *FunctionOptions
		args            []any
		want            any
		expectErr       bool
	}{
		{
			name:            "Returns When True Value",
			query:           &Query{},
			current:         Map{},
			functionOptions: &FunctionOptions{},
			args:            []any{true, "whenTrueValue", "whenFalseValue"},
			want:            "whenTrueValue",
			expectErr:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := IfFunc(tt.query, tt.current, tt.functionOptions, tt.args)
			if tt.expectErr {
				if err == nil {
					t.Fatal("expected an error, got none")
				}
			} else {
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
				if result != tt.want {
					t.Errorf("expected %v, got %v", tt.want, result)
				}
			}
		})
	}
}

func TestConstantFunc(t *testing.T) {
	tests := []struct {
		name            string
		query           *Query
		current         Map
		functionOptions *FunctionOptions
		args            []any
		want            any
		expectErr       bool
	}{
		{
			name: "Returns Correct Constant Value",
			query: &Query{
				options: &Options{
					constants: map[string]any{
						"key1": "value1",
					},
				},
			},
			current:         Map{},
			functionOptions: &FunctionOptions{},
			args:            []any{"key1"},
			want:            "value1",
			expectErr:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ConstantFunc(tt.query, tt.current, tt.functionOptions, tt.args)
			if tt.expectErr {
				if err == nil {
					t.Fatal("expected an error, got none")
				}
			} else {
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
				if result != tt.want {
					t.Errorf("expected %v, got %v", tt.want, result)
				}
			}
		})
	}
}

func TestGetVarFunc(t *testing.T) {
	tests := []struct {
		name            string
		query           *Query
		current         Map
		functionOptions *FunctionOptions
		args            []any
		want            any
		expectErr       bool
	}{
		{
			name: "Retrieves Value When Key Exists",
			query: &Query{
				options: &Options{
					vars: map[string]interface{}{
						"testKey": "testValue",
					},
					varsMut: sync.RWMutex{},
				},
			},
			current:         Map{},
			functionOptions: &FunctionOptions{},
			args:            []any{"testKey"},
			want:            "testValue",
			expectErr:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := GetVarFunc(tt.query, tt.current, tt.functionOptions, tt.args)
			if tt.expectErr {
				if err == nil {
					t.Fatal("expected an error, got none")
				}
			} else {
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
				if result != tt.want {
					t.Errorf("expected %v, got %v", tt.want, result)
				}
			}
		})
	}
}

func TestSetVarFunc(t *testing.T) {
	tests := []struct {
		name            string
		query           *Query
		current         Map
		functionOptions *FunctionOptions
		args            []any
		want            any
		expectErr       bool
	}{
		{
			name: "Sets Variable With Valid Arguments",
			query: &Query{
				options: &Options{
					vars:    make(map[string]interface{}),
					varsMut: sync.RWMutex{},
				},
			},
			current:         Map{},
			functionOptions: &FunctionOptions{},
			args:            []any{"testKey", "testValue"},
			want:            Ommit(true),
			expectErr:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := SetVarFunc(tt.query, tt.current, tt.functionOptions, tt.args)
			if tt.expectErr {
				if err == nil {
					t.Fatal("expected an error, got none")
				}
			} else {
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
				if result != tt.want {
					t.Errorf("expected %v, got %v", tt.want, result)
				}
				if tt.query.options.vars["testKey"] != "testValue" {
					t.Errorf("variable was not set correctly")
				}
			}
		})
	}
}

func TestRaiseWhenFunc(t *testing.T) {
	tests := []struct {
		name            string
		query           *Query
		current         Map
		functionOptions *FunctionOptions
		args            []any
		want            any
		expectErr       bool
	}{
		{
			name:            "Guard Error",
			query:           &Query{},
			current:         Map{},
			functionOptions: &FunctionOptions{},
			args:            []any{},
			want:            nil,
			expectErr:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := RaiseWhenFunc(tt.query, tt.current, tt.functionOptions, tt.args)
			if tt.expectErr {
				if err == nil {
					t.Fatal("expected an error, got none")
				}
			} else {
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
				if result != tt.want {
					t.Errorf("expected %v, got %v", tt.want, result)
				}
			}
		})
	}
}

func TestRaiseFunc(t *testing.T) {
	tests := []struct {
		name            string
		query           *Query
		current         Map
		functionOptions *FunctionOptions
		args            []any
		want            any
		expectErr       bool
	}{
		{
			name:            "Error From Guard",
			query:           &Query{},
			current:         Map{},
			functionOptions: &FunctionOptions{},
			args:            []any{},
			want:            nil,
			expectErr:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := RaiseFunc(tt.query, tt.current, tt.functionOptions, tt.args)
			if tt.expectErr {
				if err == nil {
					t.Fatal("expected an error, got none")
				}
			} else {
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
				if result != tt.want {
					t.Errorf("expected %v, got %v", tt.want, result)
				}
			}
		})
	}
}

func TestHashFunc(t *testing.T) {
	tests := []struct {
		name            string
		query           *Query
		current         Map
		functionOptions *FunctionOptions
		args            []any
		want            any
		expectErr       bool
	}{
		{
			name:            "SHA1 Hash",
			query:           &Query{},
			current:         Map{},
			functionOptions: &FunctionOptions{},
			args:            []any{"test data", "sha1"},
			want:            "49ba7217227f875297310a792423b954343fc4a6",
			expectErr:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := HashFunc(tt.query, tt.current, tt.functionOptions, tt.args)
			if tt.expectErr {
				if err == nil {
					t.Fatal("expected an error, got none")
				}
			} else {
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
				if result != tt.want {
					t.Errorf("expected %v, got %v", tt.want, result)
				}
			}
		})
	}
}

func TestTimestampFunc(t *testing.T) {
	tests := []struct {
		name            string
		query           *Query
		current         Map
		functionOptions *FunctionOptions
		args            []any
		expectErr       bool
	}{
		{
			name:            "Returns Current Timestamp",
			query:           &Query{},
			current:         Map{},
			functionOptions: &FunctionOptions{},
			args:            []any{},
			expectErr:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := TimestampFunc(tt.query, tt.current, tt.functionOptions, tt.args)
			if tt.expectErr {
				if err == nil {
					t.Fatal("expected an error, got none")
				}
			} else {
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
				if result == nil {
					t.Fatal("expected a timestamp, got nil")
				}
				timestamp, ok := result.(int64)
				if !ok {
					t.Fatalf("expected result to be int64, got %T", result)
				}
				if timestamp <= 0 {
					t.Fatal("expected a positive timestamp")
				}
			}
		})
	}
}

func TestGuard(t *testing.T) {
	tests := []struct {
		name      string
		n         int
		args      []any
		expectErr bool
	}{
		{
			name:      "Exact Arguments",
			n:         3,
			args:      []any{1, "two", 3.0},
			expectErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := Guard(tt.n, tt.args)
			if tt.expectErr {
				if err == nil {
					t.Fatal("expected an error, got none")
				}
			} else {
				if err != nil {
					t.Errorf("expected no error, got %v", err)
				}
			}
		})
	}
}

func TestToFloat64(t *testing.T) {
	tests := []struct {
		name      string
		input     any
		want      float64
		expectErr bool
	}{
		{
			name:      "Convert Integer to Float64",
			input:     42,
			want:      42.0,
			expectErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ToFloat64(tt.input)
			if tt.expectErr {
				if err == nil {
					t.Fatal("expected an error, got none")
				}
			} else {
				if err != nil {
					t.Errorf("expected no error, got %v", err)
				}
				if result != tt.want {
					t.Errorf("expected %v, got %v", tt.want, result)
				}
			}
		})
	}
}

func TestToInt(t *testing.T) {
	tests := []struct {
		name      string
		input     any
		want      int
		expectErr bool
	}{
		{
			name:      "Valid String to Int",
			input:     "123",
			want:      123,
			expectErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ToInt(tt.input)
			if tt.expectErr {
				if err == nil {
					t.Fatal("expected an error, got none")
				}
			} else {
				if err != nil {
					t.Errorf("expected no error, got %v", err)
				}
				if result != tt.want {
					t.Errorf("expected %v, got %v", tt.want, result)
				}
			}
		})
	}
}
