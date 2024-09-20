package set

import (
	"fmt"
	ds "github.com/dicedb/dice/internal/datastructures"
	"math"
	"strconv"
)

type Set[T comparable] struct {
	ds.BaseDataStructure[ds.DSInterface]
	elements map[T]struct{}
}

type SetInterface[T comparable] interface {
	ds.DSInterface
	Add(elements ...T)
	Remove(elements ...T)
	Contains(element T) bool
	Elements() []T
	Size() int
}


func NewSet(values []string) (ds.DSInterface, error) {
	if len(values) == 0 {
		return nil, fmt.Errorf("values slice is empty")
	}

	s := values[0]
	var set ds.DSInterface

	// Try to parse as int
	if i, err := strconv.ParseInt(s, 10, 64); err == nil {
		switch {
		case i >= math.MinInt8 && i <= math.MaxInt8:
			se := &Set[int8]{}
			for _, s := range values {
				v, err := strconv.ParseInt(s, 10, 8)
				if err != nil {
					return nil, fmt.Errorf("error parsing %q as int8: %v", s, err)
				}
				se.elements[int8(v)] = struct{}{}
			}
			set = se
		case i >= math.MinInt16 && i <= math.MaxInt16:
			se := &Set[int16]{}
			for _, s := range values {
				v, err := strconv.ParseInt(s, 10, 16)
				if err != nil {
					return nil, fmt.Errorf("error parsing %q as int16: %v", s, err)
				}
				se.elements[int16(v)] = struct{}{}
			}
			set = se
		case i >= math.MinInt32 && i <= math.MaxInt32:
			se := &Set[int32]{}
			for _, s := range values {
				v, err := strconv.ParseInt(s, 10, 32)
				if err != nil {
					return nil, fmt.Errorf("error parsing %q as int32: %v", s, err)
				}
				se.elements[int32(v)] = struct{}{}
			}
			set = se
		default:
			se := &Set[int64]{}
			for _, s := range values {
				v, err := strconv.ParseInt(s, 10, 64)
				if err != nil {
					return nil, fmt.Errorf("error parsing %q as int64: %v", s, err)
				}
				se.elements[v] = struct{}{}
			}
			set = se
		}
	} else if u, err := strconv.ParseUint(s, 10, 64); err == nil {
		switch {
		case u <= math.MaxUint8:
			se := &Set[uint8]{}
			for _, s := range values {
				v, err := strconv.ParseUint(s, 10, 8)
				if err != nil {
					return nil, fmt.Errorf("error parsing %q as uint8: %v", s, err)
				}
				se.elements[uint8(v)] = struct{}{}
			}
			set = se
		case u <= math.MaxUint16:
			se := &Set[uint16]{}
			for _, s := range values {
				v, err := strconv.ParseUint(s, 10, 16)
				if err != nil {
					return nil, fmt.Errorf("error parsing %q as uint16: %v", s, err)
				}
				se.elements[uint16(v)] = struct{}{}
			}
			set = se
		case u <= math.MaxUint32:
			se := &Set[uint32]{}
			for _, s := range values {
				v, err := strconv.ParseUint(s, 10, 32)
				if err != nil {
					return nil, fmt.Errorf("error parsing %q as uint32: %v", s, err)
				}
				se.elements[uint32(v)] = struct{}{}
			}
			set = se
		default:
			se := &Set[uint64]{}
			for _, s := range values {
				v, err := strconv.ParseUint(s, 10, 64)
				if err != nil {
					return nil, fmt.Errorf("error parsing %q as uint64: %v", s, err)
				}
				se.elements[v] = struct{}{}
			}
			set = se
		}
	} else if f, err := strconv.ParseFloat(s, 64); err == nil {
		if f >= -math.MaxFloat32 && f <= math.MaxFloat32 {
			se := &Set[float32]{}
			for _, s := range values {
				v, err := strconv.ParseFloat(s, 32)
				if err != nil {
					return nil, fmt.Errorf("error parsing %q as float32: %v", s, err)
				}
				se.elements[float32(v)] = struct{}{}
			}
			set = se
		} else {
			se := &Set[float64]{}
			for _, s := range values {
				v, err := strconv.ParseFloat(s, 64)
				if err != nil {
					return nil, fmt.Errorf("error parsing %q as float64: %v", s, err)
				}
				se.elements[v] = struct{}{}
			}
			set = se
		}
	} else {
		se := &Set[string]{}
		for _, s := range values {
			se.elements[s] = struct{}{}
		}
		set = se
	}

	return set, nil
}

func (s *Set[T]) Add(elements ...T) {
	for _, elem := range elements {
		s.elements[elem] = struct{}{}
	}
}

func (s *Set[T]) Remove(elements ...T) {
	for _, elem := range elements {
		delete(s.elements, elem)
	}
}

func (s *Set[T]) Contains(element T) bool {
	_, exists := s.elements[element]
	return exists
}

func (s *Set[T]) Elements() []T {
	elems := make([]T, 0, len(s.elements))
	for elem := range s.elements {
		elems = append(elems, elem)
	}
	return elems
}

func (s *Set[T]) Size() int {
	return len(s.elements)
}
