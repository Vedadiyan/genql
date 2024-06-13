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
	"crypto/aes"
	"crypto/cipher"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base32"
	"encoding/base64"
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"time"
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
		if whenTrue == nil {
			return nil, nil
		}
		return *whenTrue, nil
	}
	if whenFalse == nil {
		return nil, nil
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
		return nil, nil
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

//	Raise When
//
// --------------------------------------------------
// | index |    type    |       description         |
// |-------|------------|---------------------------|
// |   0   |    bool    |        condition          |
// |   0   |     any    |          error            |
// --------------------------------------------------
func RaiseWhenFunc(query *Query, current Map, functionOptions *FunctionOptions, args []any) (any, error) {
	err := Guard(2, args)
	if err != nil {
		return nil, err
	}
	cond, err := AsType[bool](args[0])
	if err != nil {
		return nil, err
	}
	if *cond {
		return nil, fmt.Errorf(fmt.Sprintf("%v", args[1]))
	}
	return Ommit(true), nil
}

//	Raise
//
// --------------------------------------------------
// | index |    type    |       description         |
// |-------|------------|---------------------------|
// |   0   |     any    |          error            |
// --------------------------------------------------
func RaiseFunc(query *Query, current Map, functionOptions *FunctionOptions, args []any) (any, error) {
	err := Guard(1, args)
	if err != nil {
		return nil, err
	}
	return nil, fmt.Errorf(fmt.Sprintf("%v", args[0]))
}

//	Hash Function
//
// --------------------------------------------------
// | index |    type    |       description         |
// |-------|------------|---------------------------|
// |   0   |     any    |    data to be hashed      |
// |   1   |    string  |       hash function       |
// --------------------------------------------------
func HashFunc(query *Query, current Map, functionOptions *FunctionOptions, args []any) (any, error) {
	err := Guard(2, args)
	if err != nil {
		return nil, err
	}
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	err = enc.Encode(struct{ Data any }{Data: args[0]})
	if err != nil {
		return nil, err
	}
	hashFunction, err := AsType[string](args[1])
	if err != nil {
		return nil, err
	}
	switch strings.ToLower(*hashFunction) {
	case "sha1":
		{
			sha1 := sha1.New()
			_, err := sha1.Write(buffer.Bytes())
			if err != nil {
				return nil, err
			}
			return hex.EncodeToString(sha1.Sum(nil)), nil
		}
	case "sha256":
		{
			sha256 := sha256.New()
			_, err := sha256.Write(buffer.Bytes())
			if err != nil {
				return nil, err
			}
			return hex.EncodeToString(sha256.Sum(nil)), nil
		}
	case "sha512":
		{
			sha512 := sha512.New()
			_, err := sha512.Write(buffer.Bytes())
			if err != nil {
				return nil, err
			}
			return hex.EncodeToString(sha512.Sum(nil)), nil
		}
	case "md5":
		{
			md5 := md5.New()
			_, err := md5.Write(buffer.Bytes())
			if err != nil {
				return nil, err
			}
			return hex.EncodeToString(md5.Sum(nil)), nil
		}
	default:
		{
			return nil, UNSUPPORTED_CASE.Extend(fmt.Sprintf("%s is not supported", *hashFunction))
		}
	}
}

//	Encode Function
//
// --------------------------------------------------
// | index |    type    |       description         |
// |-------|------------|---------------------------|
// |   0   |     any    |    data to be encoded     |
// |   1   |    string  |           base            |
// --------------------------------------------------
func EncodeFunc(query *Query, current Map, functionOptions *FunctionOptions, args []any) (any, error) {
	err := Guard(2, args)
	if err != nil {
		return nil, err
	}
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	err = enc.Encode(struct{ Data any }{Data: args[0]})
	if err != nil {
		return nil, err
	}
	base, err := AsType[string](args[1])
	if err != nil {
		return nil, err
	}
	switch strings.ToLower(*base) {
	case "base64":
		{
			return base64.URLEncoding.EncodeToString(buffer.Bytes()), nil
		}
	case "base32":
		{
			return base32.StdEncoding.EncodeToString(buffer.Bytes()), nil
		}
	case "hex":
		{
			return hex.EncodeToString(buffer.Bytes()), nil
		}
	default:
		{
			return nil, UNSUPPORTED_CASE.Extend(fmt.Sprintf("%s is not supported", *base))
		}
	}
}

//	Decode Function
//
// --------------------------------------------------
// | index |    type    |       description         |
// |-------|------------|---------------------------|
// |   0   |     any    |    data to be decoded     |
// |   1   |    string  |           base            |
// --------------------------------------------------
func DecodeFunc(query *Query, current Map, functionOptions *FunctionOptions, args []any) (any, error) {
	err := Guard(2, args)
	if err != nil {
		return nil, err
	}
	var buffer bytes.Buffer
	data, err := AsType[string](args[0])
	if err != nil {
		return nil, err
	}
	base, err := AsType[string](args[1])
	if err != nil {
		return nil, err
	}
	switch strings.ToLower(*base) {
	case "base64":
		{
			bytes, err := base64.URLEncoding.DecodeString(*data)
			if err != nil {
				return nil, err
			}
			buffer.Write(bytes)
		}
	case "base32":
		{
			bytes, err := base32.StdEncoding.DecodeString(*data)
			if err != nil {
				return nil, err
			}
			buffer.Write(bytes)
		}
	case "hex":
		{
			bytes, err := hex.DecodeString(*data)
			if err != nil {
				return nil, err
			}
			buffer.Write(bytes)
		}
	default:
		{
			return nil, UNSUPPORTED_CASE.Extend(fmt.Sprintf("%s is not supported", *base))
		}
	}
	enc := gob.NewDecoder(&buffer)
	var decodedData struct{ Data any }
	err = enc.Decode(&decodedData)
	if err != nil {
		return nil, err
	}
	return decodedData.Data, nil
}

//	Array Function
//
// --------------------------------------------------
// | index |    type    |       description         |
// |-------|------------|---------------------------|
// |   *   |     any    |        array item         |
// --------------------------------------------------
func ArrayFunc(query *Query, current Map, functionOptions *FunctionOptions, args []any) (any, error) {
	return args, nil
}

//	Date Add Function
//
// --------------------------------------------------
// | index |    type    |       description         |
// |-------|------------|---------------------------|
// |   0   |   string   |    ISO 8601 datetime      |
// |   1   |   string   |    DAY - MONTH - YEAR     |
// |   3   |    int     |      number to add        |
// --------------------------------------------------
func DateAddFunc(query *Query, current Map, functionOptions *FunctionOptions, args []any) (any, error) {
	err := Guard(3, args)
	if err != nil {
		return nil, err
	}
	dateRaw, err := AsType[string](args[0])
	if err != nil {
		return nil, err
	}
	date, err := time.Parse("2006-01-02T15:04:05-0700", *dateRaw)
	if err != nil {
		return nil, err
	}
	segment, err := AsType[string](args[1])
	if err != nil {
		return nil, err
	}
	nRaw, err := AsType[float64](args[2])
	if err != nil {
		return nil, err
	}
	n := int(*nRaw)
	switch strings.ToLower(*segment) {
	case "day":
		{
			return date.AddDate(0, 0, n), nil
		}
	case "month":
		{
			return date.AddDate(0, n, 0), nil
		}
	case "year":
		{
			return date.AddDate(n, 0, 0), nil
		}
	}
	return nil, fmt.Errorf("unsupported operation")
}

func EncryptFunc(query *Query, current Map, functionOptions *FunctionOptions, args []any) (any, error) {
	err := Guard(2, args)
	if err != nil {
		return nil, err
	}
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	err = enc.Encode(struct{ Data any }{Data: args[0]})
	if err != nil {
		return nil, err
	}
	key, err := AsType[string](args[1])
	if err != nil {
		return nil, err
	}
	sha := sha256.New()
	_, err = sha.Write([]byte(*key))
	if err != nil {
		return nil, err
	}
	hash := sha.Sum(nil)
	aes, err := aes.NewCipher(hash)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(aes)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, 12)
	_, err = io.ReadFull(rand.Reader, nonce)
	if err != nil {
		return nil, err
	}
	bytes := buffer.Bytes()
	sealed := gcm.Seal(bytes[:0], nonce, bytes, nil)
	return base64.URLEncoding.EncodeToString(append(nonce, sealed...)), nil
}

func DecryptFunc(query *Query, current Map, functionOptions *FunctionOptions, args []any) (any, error) {
	err := Guard(2, args)
	if err != nil {
		return nil, err
	}
	secret, err := AsType[string](args[0])
	if err != nil {
		return nil, err
	}
	key, err := AsType[string](args[1])
	if err != nil {
		return nil, err
	}
	sha := sha256.New()
	_, err = sha.Write([]byte(*key))
	if err != nil {
		return nil, err
	}
	hash := sha.Sum(nil)
	aes, err := aes.NewCipher(hash)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(aes)
	if err != nil {
		return nil, err
	}
	secretBytes, err := base64.URLEncoding.DecodeString(*secret)
	if err != nil {
		return nil, err
	}
	sealed, err := gcm.Open(secretBytes[:0], secretBytes[:12], secretBytes[12:], nil)
	if err != nil {
		return nil, err
	}
	buffer := bytes.NewBuffer(sealed)
	enc := gob.NewDecoder(buffer)
	var decodedData struct{ Data any }
	err = enc.Decode(&decodedData)
	if err != nil {
		return nil, err
	}
	return decodedData.Data, nil
}

// Timestamp Function
func TimestampFunc(query *Query, current Map, functionOptions *FunctionOptions, args []any) (any, error) {
	err := Guard(0, args)
	if err != nil {
		return nil, err
	}
	return time.Now().UnixNano(), nil
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
	RegisterImmediateFunction("raise_when", RaiseWhenFunc)
	RegisterImmediateFunction("raise", RaiseFunc)
	RegisterFunction("hash", HashFunc)
	RegisterFunction("encode", EncodeFunc)
	RegisterFunction("decode", DecodeFunc)
	RegisterImmediateFunction("timestamp", TimestampFunc)
	RegisterFunction("array", ArrayFunc)
	RegisterImmediateFunction("dateadd", DateAddFunc)
}
