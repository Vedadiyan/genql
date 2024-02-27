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
	"math"
	"strconv"
	"strings"
)

//	Calculates the sum of a given numeric array
//
// --------------------------------------------------
// | index |    type    |       description         |
// |-------|------------|---------------------------|
// |   0   |    []any   | must contain numbers only |
// --------------------------------------------------
func SumFunc(query *Query, current Map, functionOptions *FunctionOptions, args []any) (any, error) {
	err := Guard(1, args)
	if err != nil {
		return nil, err
	}
	slice, err := AsType[[]any](args[0])
	if err != nil {
		return nil, err
	}
	sum := float64(0)
	for _, item := range *slice {
		number, err := ToFloat64(item)
		if err != nil {
			return nil, err
		}
		sum += number
	}
	return sum, nil
}

//	Calculates the average of a given numeric array
//
// --------------------------------------------------
// | index |    type    |       description         |
// |-------|------------|---------------------------|
// |   0   |    []any   | must contain numbers only |
// --------------------------------------------------
func AvgFunc(query *Query, current Map, functionOptions *FunctionOptions, args []any) (any, error) {
	err := Guard(1, args)
	if err != nil {
		return nil, err
	}
	slice, err := AsType[[]any](args[0])
	if err != nil {
		return nil, err
	}
	sum := float64(0)
	for _, item := range *slice {
		number, err := ToFloat64(item)
		if err != nil {
			return nil, err
		}
		sum += number
	}
	sum /= float64(len(*slice))
	return sum, nil
}

//	Finds the minimum number in a given numeric array
//
// --------------------------------------------------
// | index |    type    |       description         |
// |-------|------------|---------------------------|
// |   0   |    []any   | must contain numbers only |
// --------------------------------------------------
func MinFunc(query *Query, current Map, functionOptions *FunctionOptions, args []any) (any, error) {
	err := Guard(1, args)
	if err != nil {
		return nil, err
	}
	slice, err := AsType[[]any](args[0])
	if err != nil {
		return nil, err
	}
	min := math.MaxFloat64
	for _, item := range *slice {
		number, err := ToFloat64(item)
		if err != nil {
			return nil, err
		}
		if number < min {
			min = number
		}
	}
	return min, nil
}

//	Finds the maximum number in a given numeric array
//
// --------------------------------------------------
// | index |    type    |       description         |
// |-------|------------|---------------------------|
// |   0   |    []any   | must contain numbers only |
// --------------------------------------------------
func MaxFunc(query *Query, current Map, functionOptions *FunctionOptions, args []any) (any, error) {
	err := Guard(1, args)
	if err != nil {
		return nil, err
	}
	slice, err := AsType[[]any](args[0])
	if err != nil {
		return nil, err
	}
	min := -math.MaxFloat64
	for _, item := range *slice {
		number, err := ToFloat64(item)
		if err != nil {
			return nil, err
		}
		if number > min {
			min = number
		}
	}
	return min, nil
}

//	Finds the total number of items in a given numeric array
//
// --------------------------------------------------
// | index |    type    |       description         |
// |-------|------------|---------------------------|
// |   0   |    []any   | must contain numbers only |
// --------------------------------------------------
func CountFunc(query *Query, current Map, functionOptions *FunctionOptions, args []any) (any, error) {
	if len(args) == 0 {
		return len(query.processed), nil
	}
	slice, err := AsType[[]any](args[0])
	if err != nil {
		return nil, err
	}
	return len(*slice), nil
}

//	Concatenates a list of given values
//
// --------------------------------------------------
// | index |    type    |       description         |
// |-------|------------|---------------------------|
// |  All  |    any     |      can be anything      |
// --------------------------------------------------
func ConcatFunc(query *Query, current Map, functionOptions *FunctionOptions, args []any) (any, error) {
	var buffer bytes.Buffer
	for _, arg := range args {
		buffer.WriteString(fmt.Sprintf("%v", arg))
	}
	return buffer.String(), nil
}

//	Gets the first item of an array and returns nil if the array is empty
//
// --------------------------------------------------
// | index |    type    |       description         |
// |-------|------------|---------------------------|
// |   0   |    []any   |     can be any array      |
// --------------------------------------------------
func FirstFunc(query *Query, current Map, functionOptions *FunctionOptions, args []any) (any, error) {
	err := Guard(1, args)
	if err != nil {
		return nil, err
	}
	if args[0] == nil {
		return nil, nil
	}
	slice, err := AsType[[]any](args[0])
	if err != nil {
		return nil, err
	}
	if len(*slice) > 0 {
		return (*slice)[0], nil
	}
	return nil, nil
}

//	Gets the last item of an array and returns nil if the array is empty
//
// --------------------------------------------------
// | index |    type    |       description         |
// |-------|------------|---------------------------|
// |   0   |    []any   |     can be any array      |
// --------------------------------------------------
func LastFunc(query *Query, current Map, functionOptions *FunctionOptions, args []any) (any, error) {
	err := Guard(1, args)
	if err != nil {
		return nil, err
	}
	if args[0] == nil {
		return nil, nil
	}
	slice, err := AsType[[]any](args[0])
	if err != nil {
		return nil, err
	}
	len := len(*slice)
	if len > 0 {
		return (*slice)[len-1], nil
	}
	return nil, nil
}

//	Gets the given index of an array and returns nil if the array is empty
//
// --------------------------------------------------
// | index |    type    |       description         |
// |-------|------------|---------------------------|
// |   0   |    []any   |     can be any array      |
// |   1   |     int    |       given index         |
// --------------------------------------------------
func ElementAtFunc(query *Query, current Map, functionOptions *FunctionOptions, args []any) (any, error) {
	err := Guard(2, args)
	if err != nil {
		return nil, err
	}
	if args[0] == nil {
		return nil, nil
	}
	slice, err := AsType[[]any](args[0])
	if err != nil {
		return nil, err
	}
	indexRaw, err := AsType[float64](args[1])
	if err != nil {
		return nil, err
	}
	index := int(*indexRaw)
	if len(*slice) > index {
		return (*slice)[index], nil
	}
	return nil, EXPECTATION_FAILED.Extend(fmt.Sprintf("index %d is out of range", index))
}

//	Gets the value of the only key available in an object and returns an error if multiple
//	keys are found
//
// --------------------------------------------------
// | index |    type    |       description         |
// |-------|------------|---------------------------|
// |   0   |     Map    |            --             |
// --------------------------------------------------
func DefaultKeyFunc(query *Query, current Map, functionOptions *FunctionOptions, args []any) (any, error) {
	err := Guard(1, args)
	if err != nil {
		return nil, err
	}
	if args[0] == nil {
		return nil, nil
	}
	obj, err := AsType[Map](args[0])
	if err != nil {
		return nil, err
	}
	if len(*obj) > 1 {
		return nil, fmt.Errorf("multiple keys are found")
	}
	for _, value := range *obj {
		return value, nil
	}
	return nil, fmt.Errorf("no key was found")
}

//	Converts one type to another and returns an error if conversion is not possible
//
// --------------------------------------------------
// | index |    type    |       description         |
// |-------|------------|---------------------------|
// |   0   |     any    |   value to be converted   |
// |   1   |    string  |  type to convert value to |
// --------------------------------------------------
func ChangeTypeFunc(query *Query, current Map, functionOptions *FunctionOptions, args []any) (any, error) {
	err := Guard(2, args)
	if err != nil {
		return nil, err
	}
	value, err := AsType[any](args[0])
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nil
	}
	conversionType, err := AsType[string](args[1])
	if err != nil {
		return nil, err
	}
	switch strings.ToLower(*conversionType) {
	case "array":
		{
			return []any{*value}, nil
		}
	case "string":
		{
			return fmt.Sprintf("%v", *value), nil
		}
	case "double":
		{
			return ToFloat64(*value)
		}
	case "integer":
		{
			return ToInt(*value)
		}
	default:
		{
			return nil, UNSUPPORTED_CASE.Extend(fmt.Sprintf("%s is not a valid conversion type", *conversionType))
		}
	}
}

//	Flattens an array by one dimension
//
// --------------------------------------------------
// | index |    type    |       description         |
// |-------|------------|---------------------------|
// |   0   |    []any   |      can be any array     |
// --------------------------------------------------
func UnwindFunc(query *Query, current Map, functionOptions *FunctionOptions, args []any) (any, error) {
	err := Guard(1, args)
	if err != nil {
		return nil, err
	}
	if args[0] == nil {
		return nil, nil
	}
	slice, err := AsType[[]any](args[0])
	if err != nil {
		return nil, err
	}
	output := make([]any, 0)
	for _, item := range *slice {
		if item, ok := item.([]any); ok {
			output = append(output, item...)
			continue
		}
		output = append(output, item)
	}
	return output, nil
}

//	If condition
//
// --------------------------------------------------
// | index |    type    |       description         |
// |-------|------------|---------------------------|
// |   0   |    bool    |        condition          |
// |   1   |    any     |     result when true      |
// |   2   |    any     |     result when false     |
// --------------------------------------------------
func IfFunc(query *Query, current Map, functionOptions *FunctionOptions, args []any) (any, error) {
	err := Guard(3, args)
	if err != nil {
		return nil, err
	}
	condition, err := AsType[bool](args[0])
	if err != nil {
		return nil, err
	}
	whenTrue, err := AsType[any](args[1])
	if err != nil {
		return nil, err
	}
	whenFalse, err := AsType[any](args[2])
	if err != nil {
		return nil, err
	}
	if *condition {
		return *whenTrue, nil
	}
	return *whenFalse, nil
}

//	Fuse
//
// --------------------------------------------------
// | index |    type    |       description         |
// |-------|------------|---------------------------|
// |   0   |     any    |    result to be fused     |
// --------------------------------------------------
func FuseFunc(query *Query, current Map, functionOptions *FunctionOptions, args []any) (any, error) {
	err := Guard(1, args)
	if err != nil {
		return nil, err
	}
	if args[0] == nil {
		return nil, nil
	}
	rs, err := AsType[any](args[0])
	if err != nil {
		return nil, err
	}
	switch rs := (*rs).(type) {
	case map[string]any:
		{
			return Fuse(rs), nil
		}
	default:
		{
			return nil, EXPECTATION_FAILED.Extend(fmt.Sprintf("fuse cannot be used with %T type", rs))
		}
	}
}

//	Date Range
//
// --------------------------------------------------
// | index |    type    |       description         |
// |-------|------------|---------------------------|
// |   0   |     any    |           from            |
// |   1   |     any    |            to             |
// --------------------------------------------------
func DateRangeFunc(query *Query, current Map, functionOptions *FunctionOptions, args []any) (any, error) {
	err := Guard(2, args)
	if err != nil {
		return nil, err
	}
	var (
		from string
		to   string
	)
	if args[0] != nil {
		from = fmt.Sprintf("%v", args[0])
	}
	if args[1] != nil {
		from = fmt.Sprintf("%v", args[1])
	}
	return []string{from, to}, nil
}

//	Constant
//
// --------------------------------------------------
// | index |    type    |       description         |
// |-------|------------|---------------------------|
// |   0   |   string   |           name            |
// --------------------------------------------------
func ConstantFunc(query *Query, current Map, functionOptions *FunctionOptions, args []any) (any, error) {
	err := Guard(1, args)
	if err != nil {
		return nil, err
	}
	if query.options.constants == nil {
		return nil, fmt.Errorf("constants not initialized")
	}
	key := fmt.Sprintf("%v", args[0])
	value, ok := query.options.constants[key]
	if !ok {
		return nil, fmt.Errorf("no constant by the name `%s` was found", key)
	}
	return value, nil
}

//	Get Variable
//
// --------------------------------------------------
// | index |    type    |       description         |
// |-------|------------|---------------------------|
// |   0   |   string   |           name            |
// --------------------------------------------------
func GetVarFunc(query *Query, current Map, functionOptions *FunctionOptions, args []any) (any, error) {
	err := Guard(1, args)
	if err != nil {
		return nil, err
	}
	key := fmt.Sprintf("%v", args[0])
	query.options.varsMut.RLock()
	defer query.options.varsMut.RUnlock()
	value, ok := query.options.vars[key]
	if !ok {
		return nil, fmt.Errorf("no variable by the name `%s` was found", key)
	}
	return value, nil
}

//	Set Variable
//
// --------------------------------------------------
// | index |    type    |       description         |
// |-------|------------|---------------------------|
// |   0   |   string   |           name            |
// |   0   |     any    |           value           |
// --------------------------------------------------
func SetVarFunc(query *Query, current Map, functionOptions *FunctionOptions, args []any) (any, error) {
	err := Guard(2, args)
	if err != nil {
		return nil, err
	}
	key := fmt.Sprintf("%v", args[0])
	value := args[1]
	query.options.varsMut.Lock()
	defer query.options.varsMut.Unlock()
	query.options.vars[key] = value
	return Ommit(true), nil
}

func Guard(n int, args []any) error {
	if len(args) < n {
		return fmt.Errorf("too few arguments")
	}
	if len(args) > n {
		return fmt.Errorf("too many arguments")
	}
	return nil
}

func ToFloat64(any any) (float64, error) {
	// This way of casting values to float64 is inefficient
	// I have used this technique to avoid writing a long
	// switch case only.
	number, err := strconv.ParseFloat(fmt.Sprintf("%v", any), 64)
	if err != nil {
		return 0, err
	}
	return number, nil
}

func ToInt(any any) (int, error) {
	// This way of casting values to float64 is inefficient
	// I have used this technique to avoid writing a long
	// switch case only.
	number, err := strconv.Atoi(fmt.Sprintf("%v", any))
	if err != nil {
		return 0, err
	}
	return number, nil
}

func init() {
	RegisterImmediateFunction("sum", SumFunc)
	RegisterImmediateFunction("avg", AvgFunc)
	RegisterImmediateFunction("min", MinFunc)
	RegisterImmediateFunction("max", MaxFunc)
	RegisterImmediateFunction("count", CountFunc)
	RegisterFunction("concat", ConcatFunc)
	RegisterFunction("first", FirstFunc)
	RegisterFunction("last", LastFunc)
	RegisterFunction("elementat", ElementAtFunc)
	RegisterFunction("defaultkey", DefaultKeyFunc)
	RegisterFunction("changetype", ChangeTypeFunc)
	RegisterFunction("unwind", UnwindFunc)
	RegisterFunction("if", IfFunc)
	RegisterImmediateFunction("fuse", FuseFunc)
	RegisterImmediateFunction("daterange", DateRangeFunc)
	RegisterImmediateFunction("constant", ConstantFunc)
	RegisterImmediateFunction("getvar", GetVarFunc)
	RegisterImmediateFunction("setvar", SetVarFunc)
}
