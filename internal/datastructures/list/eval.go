package list

import (
	"errors"
	"strconv"
	"strings"

	ds "github.com/dicedb/dice/internal/datastructures"
	diceerrors "github.com/dicedb/dice/internal/errors"
	"github.com/dicedb/dice/internal/ops"
	"github.com/dicedb/dice/internal/store"
)

const (
	defaultExpiry = -1
)

type Eval[T constraint] struct {
	store *store.Store[ds.DSInterface]
	op    *ops.Operation
}

func NewEval[T constraint](store *store.Store[ds.DSInterface], op *ops.Operation) *Eval[T] {
	return &Eval[T]{
		store: store,
		op:    op,
	}
}

func (e *Eval[T]) Evaluate() (interface{}, error) {
	switch strings.ToUpper(e.op.Cmd) {
	case "LPUSH":
		return e.LPUSH(e.op.Args)
	case "RPUSH":
		return e.RPUSH(e.op.Args)
	case "LPOP":
		return e.LPOP(e.op.Args)
	case "RPOP":
		return e.RPOP(e.op.Args)
	case "LRANGE":
		return e.LRANGE(e.op.Args)
	case "LLEN":
		return e.LLEN(e.op.Args)
	case "LINDEX":
		return e.LINDEX(e.op.Args)
	case "LSET":
		return e.LSET(e.op.Args)
	case "LTRIM":
		return e.LTRIM(e.op.Args)
	case "LREM":
		return e.LREM(e.op.Args)
	}

	return nil, nil
}

func (e *Eval[T]) LPUSH(args []string) (int64, error) {
	if len(args) < 2 {
		return 0, diceerrors.NewErrorWithMessage("LPUSH")
	}

	key := args[0]
	valueStrs := args[1:]

	list, err := e.getOrCreateList(key)
	if err != nil {
		return 0, err
	}

	for _, valueStr := range valueStrs {
		value, err := convertArgToType[T](valueStr)
		if err != nil {
			return 0, err
		}
		list.LPush(value)
	}

	return list.LLen(), nil
}

func (e *Eval[T]) RPUSH(args []string) (int64, error) {
	if len(args) < 2 {
		return 0, diceerrors.NewErrorWithMessage("RPUSH")
	}

	key := args[0]
	valueStrs := args[1:]

	list, err := e.getOrCreateList(key)
	if err != nil {
		return 0, err
	}

	for _, valueStr := range valueStrs {
		value, err := convertArgToType[T](valueStr)
		if err != nil {
			return 0, err
		}
		list.RPush(value)
	}

	return list.LLen(), nil
}

func (e *Eval[T]) LPOP(args []string) (T, error) {
	var zero T
	if len(args) != 1 {
		return zero, diceerrors.NewErrorWithMessage("LPOP")
	}

	key := args[0]

	list, err := e.getList(key)
	if err != nil {
		return zero, err
	}

	value, empty, err := list.LPop()
	if err != nil {
		return zero, err
	}

	if empty {
		e.store.Delete(key)
	}

	return value, nil
}

func (e *Eval[T]) RPOP(args []string) (T, error) {
	var zero T
	if len(args) != 1 {
		return zero, diceerrors.NewErrorWithMessage("RPOP")
	}

	key := args[0]

	list, err := e.getList(key)
	if err != nil {
		return zero, err
	}

	value, empty, err := list.RPop()
	if err != nil {
		return zero, err
	}

	if empty {
		e.store.Delete(key)
	}

	return value, nil
}

func (e *Eval[T]) LRANGE(args []string) ([]T, error) {
	if len(args) != 3 {
		return nil, diceerrors.NewErrorWithMessage("LRANGE")
	}

	key := args[0]
	start, err1 := strconv.Atoi(args[1])
	stop, err2 := strconv.Atoi(args[2])

	if err1 != nil || err2 != nil {
		return nil, errors.New("invalid index for 'LRANGE' command")
	}

	list, err := e.getList(key)
	if err != nil {
		return nil, err
	}

	values, err := list.LRange(start, stop)
	if err != nil {
		return nil, err
	}

	return values, nil
}

func (e *Eval[T]) LLEN(args []string) (int64, error) {
	if len(args) != 1 {
		return 0, diceerrors.NewErrorWithMessage("LLEN")
	}

	key := args[0]

	list, err := e.getList(key)
	if err != nil {
		return 0, err
	}

	return list.LLen(), nil
}

func (e *Eval[T]) LINDEX(args []string) (T, error) {
	var zero T
	if len(args) != 2 {
		return zero, diceerrors.NewErrorWithMessage("LINDEX")
	}

	key := args[0]
	index, err := strconv.Atoi(args[1])
	if err != nil {
		return zero, errors.New("invalid index for 'LINDEX' command")
	}

	list, err := e.getList(key)
	if err != nil {
		return zero, err
	}

	value, err := list.LIndex(index)
	if err != nil {
		return zero, err
	}

	return value, nil
}

func (e *Eval[T]) LSET(args []string) (string, error) {
	if len(args) != 3 {
		return "", diceerrors.NewErrorWithMessage("LSET")
	}

	key := args[0]
	index, err := strconv.Atoi(args[1])
	if err != nil {
		return "", errors.New("invalid index for 'LSET' command")
	}

	valueStr := args[2]
	value, err := convertArgToType[T](valueStr)
	if err != nil {
		return "", err
	}

	list, err := e.getList(key)
	if err != nil {
		return "", err
	}

	err = list.LSet(index, value)
	if err != nil {
		return "", err
	}

	return "OK", nil
}

func (e *Eval[T]) LTRIM(args []string) (string, error) {
	if len(args) != 3 {
		return "", diceerrors.NewErrorWithMessage("LTRIM")
	}

	key := args[0]
	start, err1 := strconv.Atoi(args[1])
	stop, err2 := strconv.Atoi(args[2])

	if err1 != nil || err2 != nil {
		return "", errors.New("invalid index for 'LTRIM' command")
	}

	list, err := e.getList(key)
	if err != nil {
		return "", err
	}

	err = list.LTrim(start, stop)
	if err != nil {
		return "", err
	}

	return "OK", nil
}

func (e *Eval[T]) LREM(args []string) (int64, error) {
	if len(args) != 3 {
		return 0, diceerrors.NewErrorWithMessage("LREM")
	}

	key := args[0]
	count, err := strconv.Atoi(args[1])
	if err != nil {
		return 0, errors.New("invalid count for 'LREM' command")
	}

	valueStr := args[2]
	value, err := convertArgToType[T](valueStr)
	if err != nil {
		return 0, err
	}

	list, err := e.getList(key)
	if err != nil {
		return 0, err
	}

	removedCount := list.LRem(count, value)
	return int64(removedCount), nil
}

// Helper methods

func (e *Eval[T]) getList(key string) (*List[T], error) {
	obj, exists := e.store.Get(key)
	if !exists {
		return nil, errors.New("no such key")
	}

	list, ok := obj.(*List[T])
	if !ok {
		return nil, errors.New("wrong type")
	}

	return list, nil
}

func (e *Eval[T]) getOrCreateList(key string) (*List[T], error) {
	obj, exists := e.store.Get(key)
	if !exists {
		list := &List[T]{}
		e.store.Put(key, list, defaultExpiry)
		return list, nil
	}

	list, ok := obj.(*List[T])
	if !ok {
		return nil, errors.New("wrong type")
	}

	return list, nil
}

// Conversion helper
func convertArgToType[T constraint](arg string) (T, error) {
	var zero T
	switch any(zero).(type) {
	case int:
		val, err := strconv.Atoi(arg)
		return any(val).(T), err
	case int8:
		val, err := strconv.ParseInt(arg, 10, 8)
		return any(int8(val)).(T), err
	case int16:
		val, err := strconv.ParseInt(arg, 10, 16)
		return any(int16(val)).(T), err
	case int32:
		val, err := strconv.ParseInt(arg, 10, 32)
		return any(int32(val)).(T), err
	case int64:
		val, err := strconv.ParseInt(arg, 10, 64)
		return any(val).(T), err
	case uint:
		val, err := strconv.ParseUint(arg, 10, 64)
		return any(uint(val)).(T), err
	case uint8:
		val, err := strconv.ParseUint(arg, 10, 8)
		return any(uint8(val)).(T), err
	case uint16:
		val, err := strconv.ParseUint(arg, 10, 16)
		return any(uint16(val)).(T), err
	case uint32:
		val, err := strconv.ParseUint(arg, 10, 32)
		return any(uint32(val)).(T), err
	case uint64:
		val, err := strconv.ParseUint(arg, 10, 64)
		return any(val).(T), err
	case float32:
		val, err := strconv.ParseFloat(arg, 32)
		return any(float32(val)).(T), err
	case float64:
		val, err := strconv.ParseFloat(arg, 64)
		return any(val).(T), err
	case string:
		return any(arg).(T), nil
	case []byte:
		return any([]byte(arg)).(T), nil
	default:
		return zero, errors.New("unsupported type")
	}
}
