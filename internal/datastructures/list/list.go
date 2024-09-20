package list

import (
	"bytes"
	"cmp"
	"errors"
	ds "github.com/dicedb/dice/internal/datastructures"
	"reflect"
	_ "unsafe"
)

var (
	// Ensure SDS[int] implements DSInterface
	_ ds.DSInterface     = &List[int]{}
	_ ListInterface[int] = &List[int]{}
)

type ListInterface[T constraint] interface {
	ds.DSInterface
	LPush(values ...T)
	RPush(values ...T)
	LPop() (T, bool, error)
	RPop() (T, bool, error)
	LRange(start, stop int) ([]T, error)
	LLen() int64
	LMove()
	LIndex(index int) (T, error)
	LSet(index int, value T) error
	LTrim(start, stop int) error
	LRem(count int, value T) int64
}

type constraint interface {
	cmp.Ordered | ~[]byte | ~string
}

type List[T constraint] struct {
	ds.BaseDataStructure[ds.DSInterface]
	size int64
	head *ListNode[T]
	tail *ListNode[T]
}

type ListNode[T constraint] struct {
	element T
	next    *ListNode[T]
	prev    *ListNode[T]
}

func NewLists[T constraint]() *List[T] {
	return &List[T]{
		size: 0,
		head: nil,
		tail: nil,
	}
}

func (b *List[T]) newNode(el T) *ListNode[T] {
	bn := &ListNode[T]{
		element: el,
	}
	return bn
}

func (b *List[T]) LPush(els ...T) {
	for _, el := range els {
		bn := b.newNode(el)
		b.append(bn)
	}
}

func (b *List[T]) RPush(els ...T) {
	for _, el := range els {
		bn := b.newNode(el)
		b.prepend(bn)
	}
}

func (b *List[T]) LLen() int64 {
	return b.size
}

func (b *List[T]) LMove() {
	// to be implemented later
	// this functionality might need transactional support
}

func (b *List[T]) LIndex(index int) (T, error) {
	// Handle negative indices
	if index < 0 {
		index = int(b.size) + index
	}

	if index < 0 || int64(index) >= b.size {
		return *new(T), errors.New("index out of range")
	}

	// If index is 0, return the head element
	if index == 0 {
		return b.head.element, nil
	}

	current := b.head
	for i := 0; i < index; i++ {
		current = current.next
	}

	return current.element, nil
}

func (b *List[T]) LPop() (T, bool, error) {
	el := b.head.element
	b.delete(b.head)
	return el, b.size == 0, nil
}

func (b *List[T]) RPop() (T, bool, error) {
	el := b.tail.element
	b.delete(b.tail)
	return el, b.size == 0, nil
}

func (b *List[T]) LRange(start, stop int) ([]T, error) {
	if start < 0 || stop < 0 || start > stop {
		return nil, errors.New("invalid range")
	}

	if int64(stop) >= b.size {
		stop = int(b.size - 1)
	}

	var els []T
	current := b.head
	for i := 0; i <= stop; i++ {
		if i >= start {
			els = append(els, current.element)
		}
		current = current.next
	}

	return els, nil
}

func (b *List[T]) LSet(index int, el T) error {
	if index < 0 || int64(index) >= b.size {
		return errors.New("index out of range")
	}

	current := b.head
	for i := 0; i < index; i++ {
		current = current.next
	}

	current.element = el
	return nil
}

func (b *List[T]) LTrim(start, stop int) error {
	if start < 0 || stop < 0 || start > stop {
		return errors.New("invalid range")
	}

	if int64(stop) >= b.size {
		stop = int(b.size - 1)
	}

	var els []T
	current := b.head
	for i := 0; i <= stop; i++ {
		if i >= start {
			els = append(els, current.element)
		}
		current = current.next
	}

	b.clear()
	for _, el := range els {
		bn := b.newNode(el)
		b.append(bn)
	}

	return nil
}

func (b *List[T]) LRem(count int, el T) int64 {
	var removed int64
	current := b.head
	for i := 0; i < int(b.size); i++ {
		if equal(current.element, el) {
			b.delete(current)
			removed++
			if removed == int64(count) {
				break
			}
		}
		current = current.next
	}

	return removed
}

func equal[T any](a, b T) bool {
	// First, check if the type is []byte
	if av, ok := any(a).([]byte); ok {
		if bv, ok := any(b).([]byte); ok {
			return bytes.Equal(av, bv)
		}
		return false
	}

	// Check if T is string
	if av, ok := any(a).(string); ok {
		bv, ok := any(b).(string)
		return ok && av == bv
	}
	// Fallback to reflect.DeepEqual for non-comparable types
	return reflect.DeepEqual(a, b)
}

func (b *List[T]) append(bn *ListNode[T]) {
	bn.prev = b.tail
	if b.tail != nil {
		b.tail.next = bn
	}
	b.tail = bn
	if b.head == nil {
		b.head = bn
	}
	b.size += 1
}

func (b *List[T]) prepend(bn *ListNode[T]) {
	bn.next = b.head
	if b.head != nil {
		b.head.prev = bn
	}
	b.head = bn
	if b.tail == nil {
		b.tail = bn
	}
	b.size += 1
}

func (b *List[T]) delete(bn *ListNode[T]) {
	if bn == b.head {
		b.head = bn.next
	}

	if bn == b.tail {
		b.tail = bn.prev
	}

	if bn.prev != nil {
		bn.prev.next = bn.next
	}

	if bn.next != nil {
		bn.next.prev = bn.prev
	}

	b.size -= 1
}

func (b *List[T]) clear() {
	b.head = nil
	b.tail = nil
	b.size = 0
}

// DeepCopy creates a deep copy of the byteList.
func (b *List[T]) DeepCopy() *List[T] {
	newList := NewLists[T]()
	newList.size = b.size

	if b.head == nil {
		return newList
	}

	// Start deep copying from the head node
	newList.head = b.head.deepCopyNode(nil)

	// Update the tail pointer
	current := newList.head
	for current.next != nil {
		current = current.next
	}
	newList.tail = current

	return newList
}

func (node *ListNode[T]) deepCopyNode(prevCopy *ListNode[T]) *ListNode[T] {
	if node == nil {
		return nil
	}

	// Create a new node
	newNode := &ListNode[T]{}

	// Deep copy the element
	newNode.element = deepCopyElement(node.element)

	// Set the previous pointer
	newNode.prev = prevCopy

	// Recursively copy the next node
	if node.next != nil {
		newNode.next = node.next.deepCopyNode(newNode)
	}

	return newNode
}

// Helper function to deep copy elements
func deepCopyElement[T constraint](el T) T {
	// Handle []byte separately
	if v, ok := any(el).([]byte); ok {
		newBytes := make([]byte, len(v))
		copy(newBytes, v)
		return any(newBytes).(T)
	}
	// For other types, return el directly (strings and ordered types)
	return el
}
