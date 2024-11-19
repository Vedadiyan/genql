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
	"reflect"
	"testing"
)

func TestValueOf(t *testing.T) {
	tests := []struct {
		name       string
		query      *Query
		current    Map
		columnName any
		want       any
		expectErr  bool
	}{
		{
			name:       "Valid Column Name",
			query:      &Query{},
			current:    Map{"column1": "value1"},
			columnName: ColumnName("column1"),
			want:       "value1",
			expectErr:  false,
		},
		{
			name:       "Nil Input",
			query:      &Query{},
			current:    Map{},
			columnName: nil,
			want:       nil,
			expectErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ValueOf(tt.query, tt.current, tt.columnName)
			if tt.expectErr {
				if err == nil {
					t.Fatalf("expected an error, got nil")
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

func TestAsType(t *testing.T) {
	tests := []struct {
		name      string
		input     any
		want      any
		expectErr bool
	}{
		{
			name:      "Successful Integer Cast",
			input:     42,
			want:      42,
			expectErr: false,
		},
		{
			name:      "Incompatible Type Cast",
			input:     "not an int",
			want:      nil,
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := AsType[int](tt.input)
			if tt.expectErr {
				if err == nil {
					t.Fatal("expected an error, got nil")
				}
				if err != INVALID_CAST {
					t.Fatalf("expected INVALID_CAST error, got %v", err)
				}
			} else {
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
				if result == nil || *result != tt.want {
					t.Fatalf("expected %v, got %v", tt.want, result)
				}
			}
		})
	}
}

func TestAsArray(t *testing.T) {
	tests := []struct {
		name      string
		input     any
		want      []any
		expectErr bool
	}{
		{
			name:      "Converts Slice to Interface Slice",
			input:     []int{1, 2, 3},
			want:      []any{1, 2, 3},
			expectErr: false,
		},
		{
			name:      "Handles Empty Slices and Arrays",
			input:     []int{},
			want:      []any{},
			expectErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := AsArray(tt.input)
			if tt.expectErr {
				if err == nil {
					t.Fatalf("expected an error, got nil")
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
