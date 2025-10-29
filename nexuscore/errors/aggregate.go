/*
该包是一个增强型错误处理工具，主要用于管理多个错误的聚合与处理，解决 Go 原生错误类型难以表示 "一组错误" 的问题。核心功能包括：
定义Aggregate接口，用于表示包含多个错误的聚合错误，支持检查特定错误是否存在；
提供创建聚合错误的NewAggregate函数，可过滤空错误并处理嵌套聚合；
支持错误过滤（FilterOut）、扁平化嵌套聚合（Flatten）、从错误计数映射创建聚合（CreateAggregateFromMessageCountMap）等操作；
提供并行执行函数并收集错误的AggregateGoroutines，简化多协程错误处理；
定义ErrPreconditionViolated常量，表示前置条件被违反的错误。
二、核心流程
1. 聚合错误的创建（NewAggregate）
输入：一个错误切片（[]error）；
处理：
过滤切片中的nil错误（避免空指针问题）；
若过滤后切片为空，返回nil；
否则，将非空错误切片包装为aggregate结构体（实现Aggregate接口）并返回。
2. 聚合错误的错误信息生成（aggregate.Error()）
作用：将聚合中的所有错误信息合并为一个字符串，去重后返回；
流程：
若聚合仅含一个错误，直接返回该错误的信息；
若含多个错误，使用StringSet记录已见过的错误信息，避免重复；
合并去重后的错误信息，多个错误时用[]包裹（如[err1, err2]）。
3. 检查特定错误是否存在（aggregate.Is()）
作用：判断聚合中是否包含目标错误（支持嵌套聚合的递归检查）；
流程：
递归遍历聚合中的每个错误（若子错误也是Aggregate，继续递归）；
对每个错误调用errors.Is(err, target)，若匹配则返回true；
遍历完所有错误仍无匹配，返回false。
4. 错误过滤（FilterOut）
作用：从错误（或聚合错误）中移除符合匹配器（Matcher）条件的错误；
流程：
若输入是Aggregate，递归处理其包含的所有错误；
对每个错误，检查是否匹配任何Matcher，不匹配则保留；
将保留的错误重新包装为聚合错误（若仅一个错误则直接返回该错误）。
5. 扁平化嵌套聚合（Flatten）
作用：将嵌套的Aggregate（如聚合中包含另一个聚合）展平为单层聚合；
流程：
递归遍历聚合中的每个错误，若子错误是Aggregate，继续扁平化；
收集所有非Aggregate的错误，组成新的聚合并返回。
6. 并行执行函数并收集错误（AggregateGoroutines）
作用：同时执行多个函数，收集所有非nil错误并聚合；
流程：
创建带缓冲的通道，用于接收每个函数的返回错误；
为每个函数启动协程，执行后将错误发送到通道；
从通道接收所有错误，过滤nil后通过NewAggregate创建聚合错误。
*/

package errors

import (
	"errors"
	"fmt"
)

// MessageCountMap contains occurrence for each error message.
type MessageCountMap map[string]int

// Aggregate represents an object that contains multiple errors, but does not
// necessarily have singular semantic meaning.
// The aggregate can be used with `errors.Is()` to check for the occurrence of
// a specific error type.
// Errors.As() is not supported, because the caller presumably cares about a
// specific error of potentially multiple that match the given type.
type Aggregate interface {
	error
	Errors() []error
	Is(error) bool
}

// NewAggregate converts a slice of errors into an Aggregate interface, which
// is itself an implementation of the error interface.  If the slice is empty,
// this returns nil.
// It will check if any of the element of input error list is nil, to avoid
// nil pointer panic when call Error().
func NewAggregate(errlist []error) Aggregate {
	if len(errlist) == 0 {
		return nil
	}
	// In case of input error list contains nil
	var errs []error
	for _, e := range errlist {
		if e != nil {
			errs = append(errs, e)
		}
	}
	if len(errs) == 0 {
		return nil
	}
	return aggregate(errs)
}

// This helper implements the error and Errors interfaces.  Keeping it private
// prevents people from making an aggregate of 0 errors, which is not
// an error, but does satisfy the error interface.
type aggregate []error

// Error is part of the error interface.
func (agg aggregate) Error() string {
	if len(agg) == 0 {
		// This should never happen, really.
		return ""
	}
	if len(agg) == 1 {
		return agg[0].Error()
	}
	seenerrs := NewString()
	result := ""
	agg.visit(func(err error) bool {
		msg := err.Error()
		if seenerrs.Has(msg) {
			return false
		}
		seenerrs.Insert(msg)
		if len(seenerrs) > 1 {
			result += ", "
		}
		result += msg
		return false
	})
	if len(seenerrs) == 1 {
		return result
	}
	return "[" + result + "]"
}

func (agg aggregate) Is(target error) bool {
	return agg.visit(func(err error) bool {
		return errors.Is(err, target)
	})
}

func (agg aggregate) visit(f func(err error) bool) bool {
	for _, err := range agg {
		switch err := err.(type) {
		case aggregate:
			if match := err.visit(f); match {
				return match
			}
		case Aggregate:
			for _, nestedErr := range err.Errors() {
				if match := f(nestedErr); match {
					return match
				}
			}
		default:
			if match := f(err); match {
				return match
			}
		}
	}

	return false
}

// Errors is part of the Aggregate interface.
func (agg aggregate) Errors() []error {
	return []error(agg)
}

// Matcher is used to match errors.  Returns true if the error matches.
type Matcher func(error) bool

// FilterOut removes all errors that match any of the matchers from the input
// error.  If the input is a singular error, only that error is tested.  If the
// input implements the Aggregate interface, the list of errors will be
// processed recursively.
//
// This can be used, for example, to remove known-OK errors (such as io.EOF or
// os.PathNotFound) from a list of errors.
func FilterOut(err error, fns ...Matcher) error {
	if err == nil {
		return nil
	}
	if agg, ok := err.(Aggregate); ok {
		return NewAggregate(filterErrors(agg.Errors(), fns...))
	}
	if !matchesError(err, fns...) {
		return err
	}
	return nil
}

// matchesError returns true if any Matcher returns true
func matchesError(err error, fns ...Matcher) bool {
	for _, fn := range fns {
		if fn(err) {
			return true
		}
	}
	return false
}

// filterErrors returns any errors (or nested errors, if the list contains
// nested Errors) for which all fns return false. If no errors
// remain a nil list is returned. The resulting silec will have all
// nested slices flattened as a side effect.
func filterErrors(list []error, fns ...Matcher) []error {
	result := []error{}
	for _, err := range list {
		r := FilterOut(err, fns...)
		if r != nil {
			result = append(result, r)
		}
	}
	return result
}

// Flatten takes an Aggregate, which may hold other Aggregates in arbitrary
// nesting, and flattens them all into a single Aggregate, recursively.
func Flatten(agg Aggregate) Aggregate {
	result := []error{}
	if agg == nil {
		return nil
	}
	for _, err := range agg.Errors() {
		if a, ok := err.(Aggregate); ok {
			r := Flatten(a)
			if r != nil {
				result = append(result, r.Errors()...)
			}
		} else {
			if err != nil {
				result = append(result, err)
			}
		}
	}
	return NewAggregate(result)
}

// CreateAggregateFromMessageCountMap converts MessageCountMap Aggregate
func CreateAggregateFromMessageCountMap(m MessageCountMap) Aggregate {
	if m == nil {
		return nil
	}
	result := make([]error, 0, len(m))
	for errStr, count := range m {
		var countStr string
		if count > 1 {
			countStr = fmt.Sprintf(" (repeated %v times)", count)
		}
		result = append(result, fmt.Errorf("%v%v", errStr, countStr))
	}
	return NewAggregate(result)
}

// Reduce will return err or, if err is an Aggregate and only has one item,
// the first item in the aggregate.
func Reduce(err error) error {
	if agg, ok := err.(Aggregate); ok && err != nil {
		switch len(agg.Errors()) {
		case 1:
			return agg.Errors()[0]
		case 0:
			return nil
		}
	}
	return err
}

// AggregateGoroutines runs the provided functions in parallel, stuffing all
// non-nil errors into the returned Aggregate.
// Returns nil if all the functions complete successfully.
func AggregateGoroutines(funcs ...func() error) Aggregate {
	errChan := make(chan error, len(funcs))
	for _, f := range funcs {
		go func(f func() error) { errChan <- f() }(f)
	}
	errs := make([]error, 0)
	for i := 0; i < cap(errChan); i++ {
		if err := <-errChan; err != nil {
			errs = append(errs, err)
		}
	}
	return NewAggregate(errs)
}

// ErrPreconditionViolated is returned when the precondition is violated
var ErrPreconditionViolated = errors.New("precondition is violated")
