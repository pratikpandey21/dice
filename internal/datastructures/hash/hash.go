// hash.go

package hash

import (
	"cmp"
	"errors"
	ds "github.com/dicedb/dice/internal/datastructures"
	"strconv"
)

// KeyConstraint Define type constraints for keys
type KeyConstraint interface {
	cmp.Ordered
}

// ValueConstraint Define type constraints for values
type ValueConstraint interface {
	any
}

var (
	_ ds.DSInterface = &Hash[string, any]{}
)

// Hash represents a generic hash map data structure
type Hash[K KeyConstraint, V ValueConstraint] struct {
	ds.BaseDataStructure[ds.DSInterface]
	data map[K]V
}

// NewHash creates a new Hash instance
func NewHash[K KeyConstraint, V ValueConstraint]() *Hash[K, V] {
	return &Hash[K, V]{
		data: make(map[K]V),
	}
}

// HSet sets the value of a field in the hash
func (h *Hash[K, V]) HSet(field K, value V) int {
	_, exists := h.data[field]
	h.data[field] = value
	if exists {
		return 0
	}
	return 1
}

// HGet gets the value of a field in the hash
func (h *Hash[K, V]) HGet(field K) (V, bool) {
	value, exists := h.data[field]
	return value, exists
}

// HDel deletes one or more fields from the hash
func (h *Hash[K, V]) HDel(fields ...K) int {
	deleted := 0
	for _, field := range fields {
		if _, exists := h.data[field]; exists {
			delete(h.data, field)
			deleted++
		}
	}
	return deleted
}

// HExists checks if a field exists in the hash
func (h *Hash[K, V]) HExists(field K) bool {
	_, exists := h.data[field]
	return exists
}

// HLen returns the number of fields in the hash
func (h *Hash[K, V]) HLen() int {
	return len(h.data)
}

// HKeys returns all the field names in the hash
func (h *Hash[K, V]) HKeys() []K {
	keys := make([]K, 0, len(h.data))
	for key := range h.data {
		keys = append(keys, key)
	}
	return keys
}

// HVals returns all the values in the hash
func (h *Hash[K, V]) HVals() []V {
	vals := make([]V, 0, len(h.data))
	for _, value := range h.data {
		vals = append(vals, value)
	}
	return vals
}

// HGetAll returns all the fields and values in the hash
func (h *Hash[K, V]) HGetAll() map[K]V {
	// Return a copy to prevent external modification
	copy := make(map[K]V, len(h.data))
	for k, v := range h.data {
		copy[k] = v
	}
	return copy
}

// HIncrBy increments the integer value of a field by a given amount
func (h *Hash[K, V]) HIncrBy(field K, increment int64) (int64, error) {
	var current int64
	value, exists := h.data[field]
	if exists {
		switch v := any(value).(type) {
		case int64:
			current = v
		case string:
			var err error
			current, err = strconv.ParseInt(v, 10, 64)
			if err != nil {
				return 0, errors.New("hash value is not an integer")
			}
		default:
			return 0, errors.New("hash value is not an integer")
		}
	}
	current += increment
	h.data[field] = any(current).(V)
	return current, nil
}

// HIncrByFloat increments the float value of a field by a given amount
func (h *Hash[K, V]) HIncrByFloat(field K, increment float64) (float64, error) {
	var current float64
	value, exists := h.data[field]
	if exists {
		switch v := any(value).(type) {
		case float64:
			current = v
		case string:
			var err error
			current, err = strconv.ParseFloat(v, 64)
			if err != nil {
				return 0, errors.New("hash value is not a float")
			}
		default:
			return 0, errors.New("hash value is not a float")
		}
	}
	current += increment
	h.data[field] = any(current).(V)
	return current, nil
}
