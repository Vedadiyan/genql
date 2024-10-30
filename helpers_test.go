package genql

import (
	"reflect"
	"testing"
)

func TestValueOfWithValidColumnName(t *testing.T) {
	query := &Query{}
	current := Map{"column1": "value1"}
	columnName := ColumnName("column1")

	result, err := ValueOf(query, current, columnName)

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	expected := "value1"
	if result != expected {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

func TestValueOfWithNilInput(t *testing.T) {
	query := &Query{}
	current := Map{}

	result, err := ValueOf(query, current, nil)

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if result != nil {
		t.Errorf("expected nil, got %v", result)
	}
}

func TestSuccessfulIntegerCast(t *testing.T) {
	var expected int = 42
	result, err := AsType[int](42)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if result == nil || *result != expected {
		t.Fatalf("expected %v, got %v", expected, result)
	}
}

func TestIncompatibleTypeCast(t *testing.T) {
	_, err := AsType[int]("not an int")
	if err == nil {
		t.Fatal("expected an error, got nil")
	}
	if err != INVALID_CAST {
		t.Fatalf("expected INVALID_CAST error, got %v", err)
	}
}


func TestConvertsSliceToInterfaceSlice(t *testing.T) {
	input := []int{1, 2, 3}
	expected := []any{1, 2, 3}

	result, err := AsArray(input)

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if !reflect.DeepEqual(result, expected) {
		t.Errorf("expected %v, got %v", expected, result)
	}
}


func TestHandlesEmptySlicesAndArrays(t *testing.T) {
	input := []int{}
	expected := []any{}

	result, err := AsArray(input)

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if !reflect.DeepEqual(result, expected) {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

