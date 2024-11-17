package genql

import (
	"math"
	"reflect"
	"sync"
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

	expectedErrorMessage := "invalid cast"
	if err.Error() != expectedErrorMessage {
		t.Errorf("expected error message to be '%v', got '%v'", expectedErrorMessage, err.Error())
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
	args := []any{[]any{3.5, 2, 4.8, 1.9}}

	result, err := MinFunc(query, current, functionOptions, args)

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	expected := 1.9
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

func TestElementAtFuncReturnsCorrectElement(t *testing.T) {
	query := &Query{}
	current := Map{}
	functionOptions := &FunctionOptions{}
	args := []any{[]any{"a", "b", "c"}, 1.0}

	result, err := ElementAtFunc(query, current, functionOptions, args)

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	expected := "b"
	if result != expected {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

func TestReturnsValueForSingleKeyValuePair(t *testing.T) {
	query := &Query{}
	current := Map{}
	functionOptions := &FunctionOptions{}
	args := []any{Map{"key": "value"}}

	result, err := DefaultKeyFunc(query, current, functionOptions, args)

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if result != "value" {
		t.Fatalf("expected value 'value', got %v", result)
	}
}


func TestChangeTypeFuncConvertsToArray(t *testing.T) {
	query := &Query{}
	current := Map{}
	functionOptions := &FunctionOptions{}
	args := []any{42, "array"}

	result, err := ChangeTypeFunc(query, current, functionOptions, args)

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	expected := []any{42}
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("expected %v, got %v", expected, result)
	}
}


func TestUnwindFuncWithNestedList(t *testing.T) {
	query := &Query{}
	current := Map{}
	functionOptions := &FunctionOptions{}
	args := []any{[]any{[]any{1, 2}, 3, []any{4, 5}}}

	result, err := UnwindFunc(query, current, functionOptions, args)

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	expected := []any{1, 2, 3, 4, 5}
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

func TestIfFuncReturnsWhenTrueValue(t *testing.T) {
	query := &Query{}
	current := Map{}
	functionOptions := &FunctionOptions{}
	args := []any{true, "whenTrueValue", "whenFalseValue"}

	result, err := IfFunc(query, current, functionOptions, args)

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	expected := "whenTrueValue"
	if result != expected {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

func TestReturnsCorrectConstantValue(t *testing.T) {
	query := &Query{
		options: &Options{
			constants: map[string]any{
				"key1": "value1",
			},
		},
	}
	current := Map{}
	functionOptions := &FunctionOptions{}
	args := []any{"key1"}

	result, err := ConstantFunc(query, current, functionOptions, args)

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	expected := "value1"
	if result != expected {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

func TestRetrieveValueWhenKeyExists(t *testing.T) {
	query := &Query{
		options: &Options{
			vars: map[string]interface{}{
				"testKey": "testValue",
			},
			varsMut: sync.RWMutex{},
		},
	}
	current := Map{}
	functionOptions := &FunctionOptions{}
	args := []any{"testKey"}

	value, err := GetVarFunc(query, current, functionOptions, args)

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if value != "testValue" {
		t.Fatalf("expected 'testValue', got %v", value)
	}
}

func TestSetVarFuncWithValidArguments(t *testing.T) {
	query := &Query{
		options: &Options{
			vars:    make(map[string]interface{}),
			varsMut: sync.RWMutex{},
		},
	}
	current := make(Map)
	functionOptions := &FunctionOptions{}
	args := []any{"testKey", "testValue"}

	result, err := SetVarFunc(query, current, functionOptions, args)

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	expectedResult := Ommit(true)
	if result != expectedResult {
		t.Fatalf("expected result to be %v, got %v", expectedResult, result)
	}

	if query.options.vars["testKey"] != "testValue" {
		t.Fatalf("expected query.options.vars['testKey'] to be 'testValue', got %v", query.options.vars["testKey"])
	}
}

func TestRaiseWhenFuncGuardError(t *testing.T) {
	query := &Query{}
	current := Map{}
	functionOptions := &FunctionOptions{}
	args := []any{}

	result, err := RaiseWhenFunc(query, current, functionOptions, args)

	if err == nil {
		t.Errorf("Expected error, got nil")
	}
	if result != nil {
		t.Errorf("Expected nil result, got %v", result)
	}
}

func TestRaiseFuncErrorFromGuard(t *testing.T) {
	query := &Query{}
	current := Map{}
	functionOptions := &FunctionOptions{}
	args := []any{}

	result, err := RaiseFunc(query, current, functionOptions, args)

	if err == nil {
		t.Errorf("Expected error from Guard, got nil")
	}
	if result != nil {
		t.Errorf("Expected nil result, got %v", result)
	}
}

func TestHashFuncSHA1(t *testing.T) {
	query := &Query{}
	current := Map{}
	functionOptions := &FunctionOptions{}
	args := []any{"test data", "sha1"}

	result, err := HashFunc(query, current, functionOptions, args)

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	expectedHash := "49ba7217227f875297310a792423b954343fc4a6"
	if result != expectedHash {
		t.Errorf("expected %s, got %s", expectedHash, result)
	}
}

func TestTimestampFuncReturnsCurrentTimestamp(t *testing.T) {
	query := &Query{}
	current := Map{}
	functionOptions := &FunctionOptions{}
	args := []any{}

	result, err := TimestampFunc(query, current, functionOptions, args)

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if result == nil {
		t.Fatalf("expected a timestamp, got nil")
	}

	timestamp, ok := result.(int64)
	if !ok {
		t.Fatalf("expected result to be int64, got %T", result)
	}

	if timestamp <= 0 {
		t.Fatalf("expected a positive timestamp, got %d", timestamp)
	}
}

func TestGuardExactArguments(t *testing.T) {
	n := 3
	args := []any{1, "two", 3.0}
	err := Guard(n, args)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}

func TestConvertIntegerToFloat64(t *testing.T) {
	input := 42
	expected := 42.0

	result, err := ToFloat64(input)

	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	if result != expected {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

func TestToIntWithValidString(t *testing.T) {
	result, err := ToInt("123")
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if result != 123 {
		t.Errorf("Expected 123, got %d", result)
	}
}
