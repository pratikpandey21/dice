package sortedset

import (
	"math"
	"math/rand"
	"sync"
	"time"

	"golang.org/x/exp/constraints"

	ds "github.com/dicedb/dice/internal/datastructures"
)

// Constants for the skip list
const (
	maxLevel    = 32
	probability = 0.25
)

// ElementConstraint defines the types allowed for elements in the sorted set
type ElementConstraint interface {
	constraints.Ordered
}

// Ensure SortedSet[T] implements DSInterface
var (
	_ ds.DSInterface = &SortedSet[string]{}
)

// SortedSet represents a generic sorted set data structure
type SortedSet[T ElementConstraint] struct {
	ds.BaseDataStructure[ds.DSInterface]
	mu     sync.RWMutex
	dict   map[T]*skipListNode[T]
	header *skipListNode[T]
	level  int
	length int
	rnd    *rand.Rand
}

type skipListNode[T ElementConstraint] struct {
	member  T
	score   float64
	forward []*skipListNode[T]
}

func newSkipListNode[T ElementConstraint](level int, score float64, member T) *skipListNode[T] {
	return &skipListNode[T]{
		member:  member,
		score:   score,
		forward: make([]*skipListNode[T], level),
	}
}

// NewSortedSet creates a new SortedSet instance
func NewSortedSet[T ElementConstraint]() *SortedSet[T] {
	return &SortedSet[T]{
		dict:   make(map[T]*skipListNode[T]),
		header: newSkipListNode[T](maxLevel, math.Inf(-1), *new(T)),
		level:  1,
		rnd:    rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// randomLevel generates a random level for a new node
func (ss *SortedSet[T]) randomLevel() int {
	level := 1
	for ss.rnd.Float64() < probability && level < maxLevel {
		level++
	}
	return level
}

// ZAdd adds one or more members to the sorted set, or updates their scores if they already exist
func (ss *SortedSet[T]) ZAdd(memberScores map[T]float64) int {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	added := 0
	for member, score := range memberScores {
		node, exists := ss.dict[member]
		if exists {
			// Update score if it has changed
			if node.score != score {
				ss.deleteNode(node)
				ss.insertNode(score, member)
			}
		} else {
			ss.insertNode(score, member)
			added++
		}
	}
	return added
}

// ZRem removes one or more members from the sorted set
func (ss *SortedSet[T]) ZRem(members ...T) int {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	removed := 0
	for _, member := range members {
		node, exists := ss.dict[member]
		if exists {
			ss.deleteNode(node)
			removed++
		}
	}
	return removed
}

// ZScore returns the score of a member in the sorted set
func (ss *SortedSet[T]) ZScore(member T) (float64, bool) {
	ss.mu.RLock()
	defer ss.mu.RUnlock()

	node, exists := ss.dict[member]
	if !exists {
		return 0, false
	}
	return node.score, true
}

// ZRank returns the rank of a member in the sorted set, with scores ordered from low to high
func (ss *SortedSet[T]) ZRank(member T) (int, bool) {
	ss.mu.RLock()
	defer ss.mu.RUnlock()

	node, exists := ss.dict[member]
	if !exists {
		return 0, false
	}

	index := ss.findNodeIndex(node)
	if index == -1 {
		return 0, false
	}

	return index, true
}

// ZRange returns a range of members in the sorted set, by index
func (ss *SortedSet[T]) ZRange(start, stop int, withScores bool) []interface{} {
	ss.mu.RLock()
	defer ss.mu.RUnlock()

	length := ss.length
	start, stop = adjustRangeIndices(start, stop, length)
	if start > stop || start >= length {
		return []interface{}{}
	}

	result := make([]interface{}, 0, (stop-start+1)*(1+boolToInt(withScores)))
	x := ss.header.forward[0]
	for i := 0; x != nil && i <= stop; i++ {
		if i >= start {
			result = append(result, x.member)
			if withScores {
				result = append(result, x.score)
			}
		}
		x = x.forward[0]
	}
	return result
}

// ZCard returns the number of members in the sorted set
func (ss *SortedSet[T]) ZCard() int {
	ss.mu.RLock()
	defer ss.mu.RUnlock()

	return ss.length
}

// ZRevRank returns the rank of a member in the sorted set, with scores ordered from high to low
func (ss *SortedSet[T]) ZRevRank(member T) (int, bool) {
	ss.mu.RLock()
	defer ss.mu.RUnlock()

	node, exists := ss.dict[member]
	if !exists {
		return 0, false
	}

	index := ss.findNodeIndex(node)
	if index == -1 {
		return 0, false
	}

	// Reverse rank is calculated as total length minus index minus 1
	revRank := ss.length - index - 1
	return revRank, true
}

func (ss *SortedSet[T]) insertNode(score float64, member T) {
	update := make([]*skipListNode[T], maxLevel)
	x := ss.header

	for i := ss.level - 1; i >= 0; i-- {
		for x.forward[i] != nil && (x.forward[i].score < score ||
			(x.forward[i].score == score && x.forward[i].member < member)) {
			x = x.forward[i]
		}
		update[i] = x
	}

	level := ss.randomLevel()
	if level > ss.level {
		for i := ss.level; i < level; i++ {
			update[i] = ss.header
		}
		ss.level = level
	}

	newNode := newSkipListNode[T](level, score, member)
	for i := 0; i < level; i++ {
		newNode.forward[i] = update[i].forward[i]
		update[i].forward[i] = newNode
	}

	ss.dict[member] = newNode
	ss.length++
}

func (ss *SortedSet[T]) deleteNode(node *skipListNode[T]) {
	update := make([]*skipListNode[T], maxLevel)
	x := ss.header

	for i := ss.level - 1; i >= 0; i-- {
		for x.forward[i] != nil && (x.forward[i].score < node.score ||
			(x.forward[i].score == node.score && x.forward[i].member < node.member)) {
			x = x.forward[i]
		}
		update[i] = x
	}

	for i := 0; i < ss.level; i++ {
		if update[i].forward[i] == node {
			update[i].forward[i] = node.forward[i]
		}
	}

	for ss.level > 1 && ss.header.forward[ss.level-1] == nil {
		ss.level--
	}

	delete(ss.dict, node.member)
	ss.length--
}

func (ss *SortedSet[T]) findNodeIndex(node *skipListNode[T]) int {
	ss.mu.RLock()
	defer ss.mu.RUnlock()

	rank := 0
	x := ss.header

	for i := ss.level - 1; i >= 0; i-- {
		for x.forward[i] != nil && (x.forward[i].score < node.score ||
			(x.forward[i].score == node.score && x.forward[i].member < node.member)) {
			// Accumulate the number of nodes passed at level 0
			if i == 0 {
				rank++
			}
			x = x.forward[i]
		}
	}

	// After traversal, x.forward[0] should point to the node
	if x.forward[0] != nil && x.forward[0] == node {
		return rank
	}

	// Node not found
	return -1
}

func adjustRangeIndices(start, stop, length int) (int, int) {
	if start < 0 {
		start = length + start
	}
	if stop < 0 {
		stop = length + stop
	}
	if start < 0 {
		start = 0
	}
	if stop >= length {
		stop = length - 1
	}
	return start, stop
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
