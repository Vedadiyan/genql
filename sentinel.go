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
