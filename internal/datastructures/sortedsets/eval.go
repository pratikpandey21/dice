package sortedset

import (
	"errors"
	ds "github.com/dicedb/dice/internal/datastructures"
	diceerrors "github.com/dicedb/dice/internal/errors"
	"github.com/dicedb/dice/internal/ops"
	"github.com/dicedb/dice/internal/store"
	"math"
	"strconv"
	"strings"
)

const (
	defaultExpiry = -1
)

type Eval[T ElementConstraint] struct {
	store *store.Store[ds.DSInterface]
	op    *ops.Operation
}

func NewEval[T ElementConstraint](store *store.Store[ds.DSInterface], op *ops.Operation) *Eval[T] {
	return &Eval[T]{
		store: store,
		op:    op,
	}
}

func (e *Eval[T]) Evaluate() (interface{}, error) {
	switch strings.ToUpper(e.op.Cmd) {
	case "ZADD":
		return e.ZADD(e.op.Args)
	case "ZREM":
		return e.ZREM(e.op.Args)
	case "ZSCORE":
		return e.ZSCORE(e.op.Args)
	case "ZRANK":
		return e.ZRANK(e.op.Args)
	case "ZREVRANK":
		return e.ZREVRANK(e.op.Args)
	case "ZRANGE":
		return e.ZRANGE(e.op.Args)
	case "ZREVRANGE":
		return e.ZREVRANGE(e.op.Args)
	case "ZCARD":
		return e.ZCARD(e.op.Args)
	case "ZCOUNT":
		return e.ZCOUNT(e.op.Args)
	case "ZRANGEBYSCORE":
		return e.ZRANGEBYSCORE(e.op.Args)
	case "ZREVRANGEBYSCORE":
		return e.ZREVRANGEBYSCORE(e.op.Args)
	case "ZINCRBY":
		return e.ZINCRBY(e.op.Args)
	}

	return nil, nil
}

// ZADD key [NX|XX|GT|LT] [CH] [INCR] score member [score member ...]
func (e *Eval[T]) ZADD(args []string) (interface{}, error) {
	if len(args) < 3 {
		return 0, diceerrors.NewErrorWithMessage("ZADD")
	}

	key := args[0]
	args = args[1:]

	// Parse options
	var (
		nx      bool
		xx      bool
		ch      bool
		incr    bool
		gt      bool
		lt      bool
		i       int
		options = true
	)

	for options && i < len(args) {
		arg := strings.ToUpper(args[i])
		switch arg {
		case "NX":
			nx = true
		case "XX":
			xx = true
		case "CH":
			ch = true
		case "INCR":
			incr = true
		case "GT":
			gt = true
		case "LT":
			lt = true
		default:
			options = false
			continue
		}
		i++
	}

	// Remaining arguments are score-member pairs
	if len(args[i:]) < 2 {
		return 0, errors.New("syntax error")
	}

	if (len(args[i:])%2 != 0) && !incr {
		return 0, errors.New("syntax error")
	}

	ss, err := e.getOrCreateSortedSet(key)
	if err != nil {
		return 0, err
	}

	added := 0
	changed := 0

	for i < len(args) {
		scoreStr := args[i]
		memberStr := args[i+1]
		i += 2

		score, err := strconv.ParseFloat(scoreStr, 64)
		if err != nil {
			return 0, errors.New("invalid score")
		}

		member, err := convertArgToMember[T](memberStr)
		if err != nil {
			return 0, err
		}

		// Handle options
		existingScore, exists := ss.ZScore(member)

		if nx && exists {
			continue
		}
		if xx && !exists {
			continue
		}
		if gt && exists && score <= existingScore {
			continue
		}
		if lt && exists && score >= existingScore {
			continue
		}

		if incr {
			if !exists {
				existingScore = 0
			}
			score += existingScore
		}

		res := ss.ZAdd(map[T]float64{member: score})
		if res > 0 {
			added += res
		} else {
			changed++
		}
	}

	if incr {
		// For INCR option, return the new score of the member
		// If multiple members are provided, it's an error
		if added+changed != 1 {
			return nil, errors.New("INCR option supports a single increment-element pair")
		}
		return score, nil
	}

	if ch {
		return added + changed, nil
	}

	return added, nil
}

func (e *Eval[T]) ZREM(args []string) (int, error) {
	if len(args) < 2 {
		return 0, diceerrors.NewErrorWithMessage("ZREM")
	}

	key := args[0]
	memberStrs := args[1:]

	ss, err := e.getSortedSet(key)
	if err != nil {
		return 0, err
	}

	members := make([]T, 0, len(memberStrs))
	for _, memberStr := range memberStrs {
		member, err := convertArgToMember[T](memberStr)
		if err != nil {
			return 0, err
		}
		members = append(members, member)
	}

	removed := ss.ZRem(members...)
	return removed, nil
}

func (e *Eval[T]) ZSCORE(args []string) (interface{}, error) {
	if len(args) != 2 {
		return nil, diceerrors.NewErrorWithMessage("ZSCORE")
	}

	key := args[0]
	memberStr := args[1]

	ss, err := e.getSortedSet(key)
	if err != nil {
		return nil, err
	}

	member, err := convertArgToMember[T](memberStr)
	if err != nil {
		return nil, err
	}

	score, exists := ss.ZScore(member)
	if !exists {
		return nil, nil // Return nil if member does not exist
	}

	return strconv.FormatFloat(score, 'f', -1, 64), nil
}

func (e *Eval[T]) ZRANK(args []string) (interface{}, error) {
	if len(args) != 2 {
		return nil, diceerrors.NewErrorWithMessage("ZRANK")
	}

	key := args[0]
	memberStr := args[1]

	ss, err := e.getSortedSet(key)
	if err != nil {
		return nil, err
	}

	member, err := convertArgToMember[T](memberStr)
	if err != nil {
		return nil, err
	}

	rank, exists := ss.ZRank(member)
	if !exists {
		return nil, nil // Return nil if member does not exist
	}

	return rank, nil
}

func (e *Eval[T]) ZREVRANK(args []string) (interface{}, error) {
	if len(args) != 2 {
		return nil, diceerrors.NewErrorWithMessage("ZREVRANK")
	}

	key := args[0]
	memberStr := args[1]

	ss, err := e.getSortedSet(key)
	if err != nil {
		return nil, err
	}

	member, err := convertArgToMember[T](memberStr)
	if err != nil {
		return nil, err
	}

	rank, exists := ss.ZRevRank(member)
	if !exists {
		return nil, nil // Return nil if member does not exist
	}

	return rank, nil
}

func (e *Eval[T]) ZRANGE(args []string) ([]interface{}, error) {
	if len(args) < 3 {
		return nil, diceerrors.NewErrorWithMessage("ZRANGE")
	}

	key := args[0]
	startStr := args[1]
	stopStr := args[2]
	withScores := false

	// Handle options
	for _, arg := range args[3:] {
		if strings.ToUpper(arg) == "WITHSCORES" {
			withScores = true
		} else {
			return nil, errors.New("syntax error in 'ZRANGE' command")
		}
	}

	start, err := strconv.Atoi(startStr)
	if err != nil {
		return nil, errors.New("invalid start index")
	}

	stop, err := strconv.Atoi(stopStr)
	if err != nil {
		return nil, errors.New("invalid stop index")
	}

	ss, err := e.getSortedSet(key)
	if err != nil {
		return nil, err
	}

	result := ss.ZRange(start, stop, withScores)
	return result, nil
}

func (e *Eval[T]) ZREVRANGE(args []string) ([]interface{}, error) {
	if len(args) < 3 {
		return nil, diceerrors.NewErrorWithMessage("ZREVRANGE")
	}

	key := args[0]
	startStr := args[1]
	stopStr := args[2]
	withScores := false

	// Handle options
	for _, arg := range args[3:] {
		if strings.ToUpper(arg) == "WITHSCORES" {
			withScores = true
		} else {
			return nil, errors.New("syntax error in 'ZREVRANGE' command")
		}
	}

	start, err := strconv.Atoi(startStr)
	if err != nil {
		return nil, errors.New("invalid start index")
	}

	stop, err := strconv.Atoi(stopStr)
	if err != nil {
		return nil, errors.New("invalid stop index")
	}

	ss, err := e.getSortedSet(key)
	if err != nil {
		return nil, err
	}

	result := ss.ZRevRange(start, stop, withScores)
	return result, nil
}

func (e *Eval[T]) ZCARD(args []string) (int, error) {
	if len(args) != 1 {
		return 0, diceerrors.NewErrorWithMessage("ZCARD")
	}

	key := args[0]

	ss, err := e.getSortedSet(key)
	if err != nil {
		return 0, err
	}

	count := ss.ZCard()
	return count, nil
}

func (e *Eval[T]) ZCOUNT(args []string) (int, error) {
	if len(args) != 3 {
		return 0, diceerrors.NewErrorWithMessage("ZCOUNT")
	}

	key := args[0]
	minStr := args[1]
	maxStr := args[2]

	min, minInclusive, err := parseScore(minStr)
	if err != nil {
		return 0, err
	}
	max, maxInclusive, err := parseScore(maxStr)
	if err != nil {
		return 0, err
	}

	ss, err := e.getSortedSet(key)
	if err != nil {
		return 0, err
	}

	count := ss.ZCount(min, max, minInclusive, maxInclusive)
	return count, nil
}

func (e *Eval[T]) ZRANGEBYSCORE(args []string) ([]interface{}, error) {
	if len(args) < 3 {
		return nil, diceerrors.NewErrorWithMessage("ZRANGEBYSCORE")
	}

	key := args[0]
	minStr := args[1]
	maxStr := args[2]
	withScores := false
	limit := false
	offset := 0
	count := -1

	// Handle options
	i := 3
	for i < len(args) {
		arg := strings.ToUpper(args[i])
		if arg == "WITHSCORES" {
			withScores = true
			i++
		} else if arg == "LIMIT" {
			if i+2 >= len(args) {
				return nil, errors.New("syntax error in 'ZRANGEBYSCORE' command")
			}
			limit = true
			var err error
			offset, err = strconv.Atoi(args[i+1])
			if err != nil {
				return nil, errors.New("invalid offset in 'LIMIT' option")
			}
			count, err = strconv.Atoi(args[i+2])
			if err != nil {
				return nil, errors.New("invalid count in 'LIMIT' option")
			}
			i += 3
		} else {
			return nil, errors.New("syntax error in 'ZRANGEBYSCORE' command")
		}
	}

	min, minInclusive, err := parseScore(minStr)
	if err != nil {
		return nil, err
	}
	max, maxInclusive, err := parseScore(maxStr)
	if err != nil {
		return nil, err
	}

	ss, err := e.getSortedSet(key)
	if err != nil {
		return nil, err
	}

	result := ss.ZRangeByScore(min, max, minInclusive, maxInclusive, withScores, limit, offset, count)
	return result, nil
}

func (e *Eval[T]) ZREVRANGEBYSCORE(args []string) ([]interface{}, error) {
	if len(args) < 3 {
		return nil, diceerrors.NewErrorWithMessage("ZREVRANGEBYSCORE")
	}

	key := args[0]
	maxStr := args[1]
	minStr := args[2]
	withScores := false
	limit := false
	offset := 0
	count := -1

	// Handle options
	i := 3
	for i < len(args) {
		arg := strings.ToUpper(args[i])
		if arg == "WITHSCORES" {
			withScores = true
			i++
		} else if arg == "LIMIT" {
			if i+2 >= len(args) {
				return nil, errors.New("syntax error in 'ZREVRANGEBYSCORE' command")
			}
			limit = true
			var err error
			offset, err = strconv.Atoi(args[i+1])
			if err != nil {
				return nil, errors.New("invalid offset in 'LIMIT' option")
			}
			count, err = strconv.Atoi(args[i+2])
			if err != nil {
				return nil, errors.New("invalid count in 'LIMIT' option")
			}
			i += 3
		} else {
			return nil, errors.New("syntax error in 'ZREVRANGEBYSCORE' command")
		}
	}

	min, minInclusive, err := parseScore(minStr)
	if err != nil {
		return nil, err
	}
	max, maxInclusive, err := parseScore(maxStr)
	if err != nil {
		return nil, err
	}

	ss, err := e.getSortedSet(key)
	if err != nil {
		return nil, err
	}

	result := ss.ZRevRangeByScore(max, min, maxInclusive, minInclusive, withScores, limit, offset, count)
	return result, nil
}

func (e *Eval[T]) ZINCRBY(args []string) (interface{}, error) {
	if len(args) != 3 {
		return nil, diceerrors.NewErrorWithMessage("ZINCRBY")
	}

	key := args[0]
	incrementStr := args[1]
	memberStr := args[2]

	increment, err := strconv.ParseFloat(incrementStr, 64)
	if err != nil {
		return nil, errors.New("invalid increment")
	}

	member, err := convertArgToMember[T](memberStr)
	if err != nil {
		return nil, err
	}

	ss, err := e.getOrCreateSortedSet(key)
	if err != nil {
		return nil, err
	}

	newScore := ss.ZIncrBy(member, increment)
	return strconv.FormatFloat(newScore, 'f', -1, 64), nil
}

// Helper methods

func (e *Eval[T]) getSortedSet(key string) (*SortedSet[T], error) {
	obj, exists := e.store.Get(key)
	if !exists {
		return nil, errors.New("no such key")
	}

	ss, ok := obj.(*SortedSet[T])
	if !ok {
		return nil, errors.New("wrong type")
	}

	return ss, nil
}

func (e *Eval[T]) getOrCreateSortedSet(key string) (*SortedSet[T], error) {
	obj, exists := e.store.Get(key)
	if !exists {
		ss := NewSortedSet[T]()
		e.store.Put(key, ss, defaultExpiry)
		return ss, nil
	}

	ss, ok := obj.(*SortedSet[T])
	if !ok {
		return nil, errors.New("wrong type")
	}

	return ss, nil
}

// Conversion helpers

func convertArgToMember[T ElementConstraint](arg string) (T, error) {
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
		return any(int64(val)).(T), err
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
		return any(uint64(val)).(T), err
	case float32:
		val, err := strconv.ParseFloat(arg, 32)
		return any(float32(val)).(T), err
	case float64:
		val, err := strconv.ParseFloat(arg, 64)
		return any(float64(val)).(T), err
	case string:
		return any(arg).(T), nil
	default:
		return zero, errors.New("unsupported member type")
	}
}

// Parse score boundaries for ZRANGEBYSCORE and similar commands
func parseScore(scoreStr string) (float64, bool, error) {
	inclusive := true
	if strings.HasPrefix(scoreStr, "(") {
		inclusive = false
		scoreStr = strings.TrimPrefix(scoreStr, "(")
	} else if scoreStr == "+inf" {
		return math.Inf(1), true, nil
	} else if scoreStr == "-inf" {
		return math.Inf(-1), true, nil
	}

	score, err := strconv.ParseFloat(scoreStr, 64)
	if err != nil {
		return 0, false, errors.New("invalid score value")
	}

	return score, inclusive, nil
}
