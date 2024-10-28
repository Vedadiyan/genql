package genql

import (
	"reflect"
	"testing"
)

func TestConvertsDoubleQuotesToBackticks(t *testing.T) {
	input := `He said, "Hello, World!"`
	expected := "He said, `Hello, World!`"
	result, err := DoubleQuotesToBackTick(input)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if result != expected {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

func TestHandlesEmptyString(t *testing.T) {
	input := ""
	expected := ""
	result, err := DoubleQuotesToBackTick(input)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if result != expected {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

func TestFindArrayIndexWithNestedArrays(t *testing.T) {
	input := "[[1, 2], [3, [4, 5]]]"
	expected := [][]int{{0, 6}, {1, 18}, {9, 19}, {13, 20}}
	result, err := FindArrayIndex(input)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

func TestFindArrayIndexWithUnmatchedClosingBracket(t *testing.T) {
	input := "[1, 2]]"
	_, err := FindArrayIndex(input)
	if err == nil {
		t.Fatal("expected an error for unmatched closing bracket, got nil")
	}
}

func TestFixIdiomaticArrayWithValidIndices(t *testing.T) {
	input := "some text [1,2,3] more text"
	expected := "some text ARRAY(1,2,3) more text"
	result, err := FixIdiomaticArray(input)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if result != expected {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

func TestFixIdiomaticArrayWithNoIndices(t *testing.T) {
	input := "some text without indices"
	expected := "some text without indices"
	result, err := FixIdiomaticArray(input)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if result != expected {
		t.Errorf("expected %v, got %v", expected, result)
	}
}
