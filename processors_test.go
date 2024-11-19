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

func TestDoubleQuotesToBackTick(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		want      string
		expectErr bool
	}{
		{
			name:      "Converts Double Quotes To Backticks",
			input:     `He said, "Hello, World!"`,
			want:      "He said, `Hello, World!`",
			expectErr: false,
		},
		{
			name:      "Handles Empty String",
			input:     "",
			want:      "",
			expectErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := DoubleQuotesToBackTick(tt.input)
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

func TestFindArrayIndex(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		want      [][]int
		expectErr bool
	}{
		{
			name:      "Find Indices With Nested Arrays",
			input:     "[[1, 2], [3, [4, 5]]]",
			want:      [][]int{{0, 6}, {1, 18}, {9, 19}, {13, 20}},
			expectErr: false,
		},
		{
			name:      "Unmatched Closing Bracket",
			input:     "[1, 2]]",
			want:      nil,
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := FindArrayIndex(tt.input)
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

func TestFixIdiomaticArray(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		want      string
		expectErr bool
	}{
		{
			name:      "Valid Indices",
			input:     "some text [1,2,3] more text",
			want:      "some text ARRAY(1,2,3) more text",
			expectErr: false,
		},
		{
			name:      "No Indices",
			input:     "some text without indices",
			want:      "some text without indices",
			expectErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := FixIdiomaticArray(tt.input)
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
