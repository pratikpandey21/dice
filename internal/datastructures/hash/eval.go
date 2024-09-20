package hash

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

type Eval[K KeyConstraint, V ValueConstraint] struct {
	store *store.Store[ds.DSInterface]
	op    *ops.Operation
}

func NewEval[K KeyConstraint, V ValueConstraint](store *store.Store[ds.DSInterface], op *ops.Operation) *Eval[K, V] {
	return &Eval[K, V]{
		store: store,
		op:    op,
	}
}

func (e *Eval[K, V]) Evaluate() (interface{}, error) {
	switch strings.ToUpper(e.op.Cmd) {
	case "HSET":
		return e.HSET(e.op.Args)
	case "HGET":
		return e.HGET(e.op.Args)
	case "HDEL":
		return e.HDEL(e.op.Args)
	case "HEXISTS":
		return e.HEXISTS(e.op.Args)
	case "HLEN":
		return e.HLEN(e.op.Args)
	case "HKEYS":
		return e.HKEYS(e.op.Args)
	case "HVALS":
		return e.HVALS(e.op.Args)
	case "HGETALL":
		return e.HGETALL(e.op.Args)
	case "HINCRBY":
		return e.HINCRBY(e.op.Args)
	case "HINCRBYFLOAT":
		return e.HINCRBYFLOAT(e.op.Args)
	}

	return nil, nil
}

func (e *Eval[K, V]) HSET(args []string) (int, error) {
	if len(args) < 3 || len(args)%2 != 1 {
		return 0, diceerrors.NewErrorWithMessage("HSET")
	}

	key := args[0]
	fieldsAndValues := args[1:]

	hash, err := e.getOrCreateHash(key)
	if err != nil {
		return 0, err
	}

	added := 0
	for i := 0; i < len(fieldsAndValues); i += 2 {
		fieldStr := fieldsAndValues[i]
		valueStr := fieldsAndValues[i+1]

		field, err := convertArgToKey[K](fieldStr)
		if err != nil {
			return 0, err
		}

		value, err := convertArgToValue[V](valueStr)
		if err != nil {
			return 0, err
		}

		res := hash.HSet(field, value)
		added += res
	}

	return added, nil
}

func (e *Eval[K, V]) HGET(args []string) (V, error) {
	var zero V
	if len(args) != 2 {
		return zero, diceerrors.NewErrorWithMessage("HGET")
	}

	key := args[0]
	fieldStr := args[1]

	field, err := convertArgToKey[K](fieldStr)
	if err != nil {
		return zero, err
	}

	hash, err := e.getHash(key)
	if err != nil {
		return zero, err
	}

	value, exists := hash.HGet(field)
	if !exists {
		return zero, nil // Field does not exist
	}

	return value, nil
}

func (e *Eval[K, V]) HDEL(args []string) (int, error) {
	if len(args) < 2 {
		return 0, diceerrors.NewErrorWithMessage("HDEL")
	}

	key := args[0]
	fieldStrs := args[1:]

	hash, err := e.getHash(key)
	if err != nil {
		return 0, err
	}

	fields := make([]K, 0, len(fieldStrs))
	for _, fieldStr := range fieldStrs {
		field, err := convertArgToKey[K](fieldStr)
		if err != nil {
			return 0, err
		}
		fields = append(fields, field)
	}

	deleted := hash.HDel(fields...)
	return deleted, nil
}

func (e *Eval[K, V]) HEXISTS(args []string) (bool, error) {
	if len(args) != 2 {
		return false, diceerrors.NewErrorWithMessage("HEXISTS")
	}

	key := args[0]
	fieldStr := args[1]

	field, err := convertArgToKey[K](fieldStr)
	if err != nil {
		return false, err
	}

	hash, err := e.getHash(key)
	if err != nil {
		return false, err
	}

	exists := hash.HExists(field)
	return exists, nil
}

func (e *Eval[K, V]) HLEN(args []string) (int, error) {
	if len(args) != 1 {
		return 0, diceerrors.NewErrorWithMessage("HLEN")
	}

	key := args[0]

	hash, err := e.getHash(key)
	if err != nil {
		return 0, err
	}

	length := hash.HLen()
	return length, nil
}

func (e *Eval[K, V]) HKEYS(args []string) ([]K, error) {
	if len(args) != 1 {
		return nil, diceerrors.NewErrorWithMessage("HKEYS")
	}

	key := args[0]

	hash, err := e.getHash(key)
	if err != nil {
		return nil, err
	}

	keys := hash.HKeys()
	return keys, nil
}

func (e *Eval[K, V]) HVALS(args []string) ([]V, error) {
	if len(args) != 1 {
		return nil, diceerrors.NewErrorWithMessage("HVALS")
	}

	key := args[0]

	hash, err := e.getHash(key)
	if err != nil {
		return nil, err
	}

	values := hash.HVals()
	return values, nil
}

func (e *Eval[K, V]) HGETALL(args []string) (map[K]V, error) {
	if len(args) != 1 {
		return nil, diceerrors.NewErrorWithMessage("HGETALL")
	}

	key := args[0]

	hash, err := e.getHash(key)
	if err != nil {
		return nil, err
	}

	data := hash.HGetAll()
	return data, nil
}

func (e *Eval[K, V]) HINCRBY(args []string) (int64, error) {
	if len(args) != 3 {
		return 0, diceerrors.NewErrorWithMessage("HINCRBY")
	}

	key := args[0]
	fieldStr := args[1]
	incrementStr := args[2]

	field, err := convertArgToKey[K](fieldStr)
	if err != nil {
		return 0, err
	}

	increment, err := strconv.ParseInt(incrementStr, 10, 64)
	if err != nil {
		return 0, errors.New("invalid increment for 'HINCRBY' command")
	}

	hash, err := e.getOrCreateHash(key)
	if err != nil {
		return 0, err
	}

	newValue, err := hash.HIncrBy(field, increment)
	if err != nil {
		return 0, err
	}

	return newValue, nil
}

func (e *Eval[K, V]) HINCRBYFLOAT(args []string) (float64, error) {
	if len(args) != 3 {
		return 0, diceerrors.NewErrorWithMessage("HINCRBYFLOAT")
	}

	key := args[0]
	fieldStr := args[1]
	incrementStr := args[2]

	field, err := convertArgToKey[K](fieldStr)
	if err != nil {
		return 0, err
	}

	increment, err := strconv.ParseFloat(incrementStr, 64)
	if err != nil {
		return 0, errors.New("invalid increment for 'HINCRBYFLOAT' command")
	}

	hash, err := e.getOrCreateHash(key)
	if err != nil {
		return 0, err
	}

	newValue, err := hash.HIncrByFloat(field, increment)
	if err != nil {
		return 0, err
	}

	return newValue, nil
}

// Helper methods

func (e *Eval[K, V]) getHash(key string) (*Hash[K, V], error) {
	obj, exists := e.store.Get(key)
	if !exists {
		return nil, errors.New("no such key")
	}

	hash, ok := obj.(*Hash[K, V])
	if !ok {
		return nil, errors.New("wrong type")
	}

	return hash, nil
}

func (e *Eval[K, V]) getOrCreateHash(key string) (*Hash[K, V], error) {
	obj, exists := e.store.Get(key)
	if !exists {
		hash := NewHash[K, V]()
		e.store.Put(key, hash, defaultExpiry)
		return hash, nil
	}

	hash, ok := obj.(*Hash[K, V])
	if !ok {
		return nil, errors.New("wrong type")
	}

	return hash, nil
}

// Conversion helpers
func convertArgToKey[K KeyConstraint](arg string) (K, error) {
	var zero K
	switch any(zero).(type) {
	case int:
		val, err := strconv.Atoi(arg)
		return any(val).(K), err
	case int8:
		val, err := strconv.ParseInt(arg, 10, 8)
		return any(int8(val)).(K), err
	case int16:
		val, err := strconv.ParseInt(arg, 10, 16)
		return any(int16(val)).(K), err
	case int32:
		val, err := strconv.ParseInt(arg, 10, 32)
		return any(int32(val)).(K), err
	case int64:
		val, err := strconv.ParseInt(arg, 10, 64)
		return any(int64(val)).(K), err
	case uint:
		val, err := strconv.ParseUint(arg, 10, 64)
		return any(uint(val)).(K), err
	case uint8:
		val, err := strconv.ParseUint(arg, 10, 8)
		return any(uint8(val)).(K), err
	case uint16:
		val, err := strconv.ParseUint(arg, 10, 16)
		return any(uint16(val)).(K), err
	case uint32:
		val, err := strconv.ParseUint(arg, 10, 32)
		return any(uint32(val)).(K), err
	case uint64:
		val, err := strconv.ParseUint(arg, 10, 64)
		return any(uint64(val)).(K), err
	case float32:
		val, err := strconv.ParseFloat(arg, 32)
		return any(float32(val)).(K), err
	case float64:
		val, err := strconv.ParseFloat(arg, 64)
		return any(float64(val)).(K), err
	case string:
		return any(arg).(K), nil
	default:
		return zero, errors.New("unsupported key type")
	}
}

func convertArgToValue[V ValueConstraint](arg string) (V, error) {
	var zero V
	switch any(zero).(type) {
	case int:
		val, err := strconv.Atoi(arg)
		return any(val).(V), err
	case int64:
		val, err := strconv.ParseInt(arg, 10, 64)
		return any(val).(V), err
	case float64:
		val, err := strconv.ParseFloat(arg, 64)
		return any(val).(V), err
	case string:
		return any(arg).(V), nil
	case []byte:
		return any([]byte(arg)).(V), nil
	default:
		// Default to string
		return any(arg).(V), nil
	}
}
