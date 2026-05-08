package handler

import (
	"errors"
	"fmt"
)

type ErrorCode string

const (
	ErrorValidation ErrorCode = "VALIDATION_ERROR"
	ErrorProcessing ErrorCode = "PROCESSING_ERROR"
)

type CodedError struct {
	Code ErrorCode
	Err  error
}

func (e CodedError) Error() string {
	return fmt.Sprintf("%s: %v", e.Code, e.Err)
}

func (e CodedError) Unwrap() error {
	return e.Err
}

func validationError(format string, args ...any) error {
	return CodedError{Code: ErrorValidation, Err: fmt.Errorf(format, args...)}
}

func ErrorCodeOf(err error) string {
	if err == nil {
		return ""
	}
	var coded CodedError
	if ok := errors.As(err, &coded); ok {
		return string(coded.Code)
	}
	return string(ErrorProcessing)
}
