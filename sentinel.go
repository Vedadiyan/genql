// Copyright 2012-2023 Pouya Vedadiyan
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

import "fmt"

type SQLError string

func (sqlError SQLError) Error() string {
	return string(sqlError)
}

func (sqlError SQLError) Extend(message string) SQLError {
	return SQLError(fmt.Sprintf("%s. %s", sqlError, message))
}

const (
	INVALID_CAST       SQLError = SQLError("invalid cast")
	UNDEFINED_OPERATOR SQLError = SQLError("undefined operator")
	INVALID_FUNCTION   SQLError = SQLError("invalid function name")
	INVALID_TYPE       SQLError = SQLError("invalid type")
	UNSUPPORTED_CASE   SQLError = SQLError("unsupported operation")
	KEY_NOT_FOUND      SQLError = SQLError("key not found")
	EXPECTATION_FAILED SQLError = SQLError("expectation failed")
)
