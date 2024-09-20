package bloom

import "github.com/dicedb/dice/internal/errors"

var (
	errInvalidErrorRateType = errors.NewErr("only float values can be provided for error rate")
	errInvalidErrorRate     = errors.NewErr("invalid error rate value provided")
	errInvalidCapacityType  = errors.NewErr("only integer values can be provided for Capacity")
	errInvalidCapacity      = errors.NewErr("invalid Capacity value provided")

	errInvalidKey = errors.NewErr("invalid key: no bloom filter found")

	errEmptyValue   = errors.NewErr("empty value provided")
	errUnableToHash = errors.NewErr("unable to hash given value")
)
