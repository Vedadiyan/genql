package genql

import (
	"math"
	"testing"
)

func TestSumFuncCalculatesSumCorrectly(t *testing.T) {
	query := &Query{}
	current := Map{}
	functionOptions := &FunctionOptions{}
	args := []any{[]any{1.0, 2.0, 3.0}}

	result, err := SumFunc(query, current, functionOptions, args)

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	expected := 6.0
	if result != expected {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

func TestSumFuncWithInvalidInput(t *testing.T) {
	query := &Query{}
	current := Map{}
	functionOptions := &FunctionOptions{}
	args := []any{"not a list"}

	_, err := SumFunc(query, current, functionOptions, args)

	if err == nil {
		t.Fatal("expected an error, got none")
	}
}

func TestAvgFuncComputesAverageCorrectly(t *testing.T) {
	query := &Query{}
	current := Map{}
	functionOptions := &FunctionOptions{}
	args := []any{[]any{1.0, 2.0, 3.0, 4.0, 5.0}}

	result, err := AvgFunc(query, current, functionOptions, args)

	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	expected := 3.0
	if result != expected {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

func TestHandleNonNumericValues(t *testing.T) {
	query := &Query{}
	current := Map{}
	functionOptions := &FunctionOptions{}
	args := []any{[]any{1, "two", 3}}

	_, err := AvgFunc(query, current, functionOptions, args)

	if err == nil {
		t.Fatal("Expected an error for non-numeric values, got nil")
	}
}

func TestMinFuncReturnsMinimumValue(t *testing.T) {
	query := &Query{}
	current := Map{}
	functionOptions := &FunctionOptions{}
	args := []any{[]any{3.5, 2.1, 4.8, 1.9}}

	result, err := MinFunc(query, current, functionOptions, args)

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	expected := 1.9
	if result != expected {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

func TestMinFuncHandlesMixedNumericTypes(t *testing.T) {
	query := &Query{}
	current := Map{}
	functionOptions := &FunctionOptions{}
	args := []any{[]any{3, 2.5, 4.8, 1}}

	result, err := MinFunc(query, current, functionOptions, args)

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	expected := 1.0
	if result != expected {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

func TestMaxFuncReturnsMaximumValue(t *testing.T) {
	query := &Query{}
	current := Map{}
	functionOptions := &FunctionOptions{}
	args := []any{[]any{1.0, 2.5, 3.7, 0.9}}

	result, err := MaxFunc(query, current, functionOptions, args)

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	expected := 3.7
	if result != expected {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

func TestMaxFuncHandlesEmptySlice(t *testing.T) {
	query := &Query{}
	current := Map{}
	functionOptions := &FunctionOptions{}
	args := []any{[]any{}}

	result, err := MaxFunc(query, current, functionOptions, args)

	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	expected := -math.MaxFloat64
	if result != expected {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

func TestCountFuncWithEmptyArgs(t *testing.T) {
	query := &Query{processed: []any{1, 2, 3}}
	current := Map{}
	functionOptions := &FunctionOptions{}
	args := []any{}

	result, err := CountFunc(query, current, functionOptions, args)

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	expected := 3
	if result != expected {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

func TestCountFuncEmptyArgs(t *testing.T) {
	query := &Query{processed: []any{}}
	current := Map{}
	functionOptions := &FunctionOptions{}
	args := []any{[]any{}}

	result, err := CountFunc(query, current, functionOptions, args)

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	expected := 0
	if result != expected {
		t.Errorf("expected %v, got %v", expected, result)
	}
}
func TestConcatFuncWithStringsAndNumbers(t *testing.T) {
	query := &Query{}
	current := Map{}
	functionOptions := &FunctionOptions{}
	args := []any{"Hello", 123, "World", 456}

	result, err := ConcatFunc(query, current, functionOptions, args)

	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	expected := "Hello123World456"
	if result != expected {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

func TestConcatFuncWithNilValues(t *testing.T) {
	query := &Query{}
	current := Map{}
	functionOptions := &FunctionOptions{}
	args := []any{"Hello", nil, "World", nil}

	result, err := ConcatFunc(query, current, functionOptions, args)

	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	expected := "Hello<nil>World<nil>"
	if result != expected {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

func TestReturnsFirstElementOfNonEmptySlice(t *testing.T) {
	query := &Query{}
	current := Map{}
	functionOptions := &FunctionOptions{}
	args := []any{[]any{1, 2, 3}}

	result, err := FirstFunc(query, current, functionOptions, args)

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if result != 1 {
		t.Fatalf("expected 1, got %v", result)
	}
}

func TestHandlesNilAsFirstElementInArgs(t *testing.T) {
	query := &Query{}
	current := Map{}
	functionOptions := &FunctionOptions{}
	args := []any{nil}

	result, err := FirstFunc(query, current, functionOptions, args)

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if result != nil {
		t.Fatalf("expected nil, got %v", result)
	}
}

func TestReturnsLastElementOfNonEmptySlice(t *testing.T) {
	query := &Query{}
	current := Map{}
	functionOptions := &FunctionOptions{}
	args := []any{[]any{1, 2, 3, 4, 5}}

	result, err := LastFunc(query, current, functionOptions, args)

	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	expected := 5
	if result != expected {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

func TestHandlesSingleElementSlice(t *testing.T) {
	query := &Query{}
	current := Map{}
	functionOptions := &FunctionOptions{}
	args := []any{[]any{42}}

	result, err := LastFunc(query, current, functionOptions, args)

	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	expected := 42
	if result != expected {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}
