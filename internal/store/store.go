package store

import (
	ds "github.com/dicedb/dice/internal/datastructures"
	"time"

	"github.com/cockroachdb/swiss"
	"github.com/dicedb/dice/internal/object"
)

// WatchEvent represents a change in a watched key.
type WatchEvent struct {
	Key       string
	Operation string
	Value     object.Obj
}

type Store[T ds.DSInterface] struct {
	store     *swiss.Map[string, T]
	expires   *swiss.Map[*T, uint64] // Does not need to be thread-safe as it is only accessed by a single thread.
	watchChan chan WatchEvent
}

func NewStore[T ds.DSInterface](watchChan chan WatchEvent) *Store[T] {
	return &Store[T]{
		store:     swiss.New[string, T](10240),
		expires:   swiss.New[*T, uint64](10240),
		watchChan: watchChan,
	}
}

func (s *Store[T]) Put(key string, Value T, expDurationMs int64) {
	Value.UpdateLastAccessedAt()
	s.store.Put(key, Value)
	if expDurationMs > 0 {
		s.expires.Put(&Value, uint64(expDurationMs))
	}
}

func (s *Store[T]) setExpiry(Value *T, expDurationMs int64) {
	s.expires.Put(Value, uint64(time.Now().UnixMilli())+uint64(expDurationMs))
}

func (s *Store[T]) Get(key string) (T, bool) {
	value, exists := s.store.Get(key)
	if exists {
		value.UpdateLastAccessedAt()
	}

	return value, exists
}

func (s *Store[T]) Delete(key string) bool {
	if _, exists := s.store.Get(key); exists {
		s.store.Delete(key)
		return exists
	}

	return false
}

func (s *Store[T]) GetExpiry(key string) (uint64, bool) {
	if val, exists := s.store.Get(key); exists {
		return s.expires.Get(&val)
	}

	return 0, false
}

func (s *Store[T]) SetExpiry(key string, expDurationMs int64) {
	obj, exists := s.store.Get(key)
	if exists {
		s.expires.Put(&obj, uint64(time.Now().UnixMilli())+uint64(expDurationMs))
	}
}
