package genql

import (
	"fmt"
	"reflect"
	"testing"
)

func TestNewIndexWithSingleInteger(t *testing.T) {
	value := 5
	indexSelector := NewIndex(value)

	if indexSelector == nil {
		t.Fatalf("Expected non-nil IndexSelector, got nil")
	}

	if indexSelector.indexSelector != value {
		t.Errorf("Expected indexSelector to be %d, got %d", value, indexSelector.indexSelector)
	}

	if indexSelector.selectorType != INDEX {
		t.Errorf("Expected selectorType to be INDEX, got %v", indexSelector.selectorType)
	}
}

func TestGetTypeReturnsNoneForEmptyString(t *testing.T) {
	pipeSelector := &PipeSelector{typeSelector: ""}
	result := pipeSelector.GetType()
	if result != NONE {
		t.Errorf("Expected NONE, got %v", result)
	}
}

func TestGetTypeReturnsUnknownForUnrecognizedValue(t *testing.T) {
	pipeSelector := &PipeSelector{typeSelector: "unrecognized"}
	result := pipeSelector.GetType()
	if result != UNKNOWN {
		t.Errorf("Expected UNKNOWN, got %v", result)
	}
}

func TestReadIndexWithValidPositiveInteger(t *testing.T) {
	input := "42"
	expected := 42
	result, err := ReadIndex(input)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if result != expected {
		t.Fatalf("expected %d, got %d", expected, result)
	}
}

func TestHandleNonNumericStringInput(t *testing.T) {
	input := "abc"
	_, err := ReadIndex(input)
	if err == nil {
		t.Fatal("expected an error, got nil")
	}
}
func TestSelectDimensionWithPositiveIndices(t *testing.T) {
	data := []any{1, 2, 3, 4, 5}
	dimensions := []*IndexSelector{
		{
			selectorType:  RANGE,
			rangeSelector: [2]int{2, 4},
		},
	}
	expected := []any{3, 4}

	result, err := SelectDimension(data, dimensions)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if !reflect.DeepEqual(result, expected) {
		t.Errorf("expected %v, got %v", expected, result)
	}
}
func TestSelectDimensionWithMixedIndices(t *testing.T) {
	data := []any{1, 2, 3, 4, 5}
	dimensions := []*IndexSelector{
		{
			selectorType:  RANGE,
			rangeSelector: [2]int{0, -1},
		},
	}
	expected := []any{1, 2, 3, 4, 5}

	result, err := SelectDimension(data, dimensions)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if !reflect.DeepEqual(result, expected) {
		t.Errorf("expected %v, got %v", expected, result)
	}
}
func TestReadRangeValidInput(t *testing.T) {
	match := "(0:10)"
	expected := NewIndex([2]int{0, 10})

	result, err := ReadRange(match)

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if *result != *expected {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

func TestReadRangeNoColonDelimiter(t *testing.T) {
	match := "(15)"

	_, err := ReadRange(match)

	if err == nil {
		t.Fatal("expected an error, got nil")
	}
}

func TestParseArrayWithMultipleIndices(t *testing.T) {
	input := "[1,2,3]"
	expected := []*IndexSelector{
		NewIndex(1),
		NewIndex(2),
		NewIndex(3),
	}
	result, err := ParseArray(input)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

func TestParseArrayHandlesEmptyString(t *testing.T) {
	input := "[]"
	expected := []*IndexSelector{}
	result, err := ParseArray(input)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

func TestParsePipeValidString(t *testing.T) {
	match := "key1|value1,key2|value2"
	expected := []*PipeSelector{
		NewPipe("key1", "value1"),
		NewPipe("key2", "value2"),
	}
	result, err := ParsePipe(match)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

func TestUnwindFlattensNestedArrayCompletely(t *testing.T) {
	data := []any{[]any{1, 2, 3}, []any{4, 5, 6}}
	expected := []any{1, 2, 3, 4, 5, 6}
	result := Unwind(data, 2)
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Expected %v, but got %v", expected, result)
	}
}
func TestRetrieveExistingKeyValuePair(t *testing.T) {
	data := map[string]any{
		"key1": "value1",
		"key2": 42,
	}
	result := SelectObject(data, "key1")
	if result != "value1" {
		t.Errorf("Expected 'value1', got %v", result)
	}
}
func TestEmptyMapInput(t *testing.T) {
	data := map[string]any{}
	result := SelectObject(data, "key1")
	if result != nil {
		t.Errorf("Expected nil, got %v", result)
	}
}

func TestExecReaderWithValidSelector(t *testing.T) {
	data := map[string]interface{}{
		"name": "John",
		"age":  30,
	}
	selector := "name"
	expected := "John"

	result, err := ExecReader(data, selector)

	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if result != expected {
		t.Errorf("Expected result %v, got %v", expected, result)
	}
}
func TestExecReaderWithEmptySelector(t *testing.T) {
	data := map[string]interface{}{
		"name": "John",
		"age":  30,
	}
	selector := ""
	expected := data

	result, err := ExecReader(data, selector)

	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Expected result %v, got %v", expected, result)
	}
}

func TestExecutesTopLevelFunctionWithValidSelector(t *testing.T) {
	data := "test data"
	selectors := []any{TopLevelFunctionSelector("validFunction")}
	topLevelFunctions = map[string]func(any) (any, error){
		"validFunction": func(input any) (any, error) {
			return fmt.Sprintf("processed %v", input), nil
		},
	}
	result, err := ReaderExecutor(data, selectors)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	expected := "processed test data"
	if result != expected {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

func TestHandlesEmptySelectorsListGracefully(t *testing.T) {
	data := "test data"
	selectors := []any{}
	result, err := ReaderExecutor(data, selectors)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if result != data {
		t.Errorf("expected %v, got %v", data, result)
	}
}
func TestReaderWithSingleKeySelector(t *testing.T) {
	data := map[string]any{
		"key1": "value1",
		"key2": "value2",
	}
	selectors := []any{KeySelector("key1")}
	result, err := Reader(data, selectors)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	expected := "value1"
	if result != expected {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

func TestReaderWithEmptySelectors(t *testing.T) {
	data := map[string]any{
		"key1": "value1",
		"key2": "value2",
	}
	selectors := []any{}
	result, err := Reader(data, selectors)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !reflect.DeepEqual(result, data) {
		t.Errorf("expected %v, got %v", data, result)
	}
}
func TestMixUnsupportedDataType(t *testing.T) {
	input := "unsupported"

	_, err := Mix(input)

	if err != UNSUPPORTED_CASE {
		t.Errorf("expected error %v, got %v", UNSUPPORTED_CASE, err)
	}
}
func TestFlattenNestedArray(t *testing.T) {
	input := []any{1, []any{2, 3}, []any{[]any{4, 5}, 6}, 7}
	expected := []any{1, 2, 3, 4, 5, 6, 7}
	result := MixArray(input)
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Expected %v, but got %v", expected, result)
	}
}
func TestMixObjectFlattensNestedMaps(t *testing.T) {
	input := map[string]any{
		"a": map[string]any{
			"b": 1,
			"c": 2,
		},
		"d": 3,
	}
	expected := map[string]any{
		"a_b": 1,
		"a_c": 2,
		"d":   3,
	}
	result, err := MixObject(input)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

func TestMixObjectHandlesEmptyMap(t *testing.T) {
	input := map[string]any{}
	expected := map[string]any{}
	result, err := MixObject(input)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

func TestDistinctRemovesDuplicates(t *testing.T) {
	input := []any{1, 2, 2, 3, 4, 4, 5}
	expected := []any{1, 2, 3, 4, 5}

	result, err := Distinct(input)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

func TestDistinctAllIdenticalElements(t *testing.T) {
	input := []any{"a", "a", "a", "a"}
	expected := []any{"a"}

	result, err := Distinct(input)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

func TestRegisterNewTopLevelFunctionSuccessfully(t *testing.T) {
	topLevelFunctions = nil
	testFunc := func(input any) (any, error) {
		return input, nil
	}
	RegisterTopLevelFunction("testFunc", testFunc)
	if _, exists := topLevelFunctions["testFunc"]; !exists {
		t.Errorf("Expected function 'testFunc' to be registered, but it was not found")
	}
}

func TestRegisterFunctionWithEmptyName(t *testing.T) {
	topLevelFunctions = nil
	testFunc := func(input any) (any, error) {
		return input, nil
	}
	RegisterTopLevelFunction("", testFunc)
	if _, exists := topLevelFunctions[""]; !exists {
		t.Errorf("Expected function with empty name to be registered, but it was not found")
	}
}
