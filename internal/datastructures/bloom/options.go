package bloom

import (
	"hash"
	"strconv"
)

type BloomOpts struct {
	ErrorRate float64 // desired error rate (the false positive rate) of the filter
	Capacity  uint64  // number of expected entries to be added to the filter

	Bits    uint64        // total number of Bits reserved for the filter
	HashFns []hash.Hash64 // array of hash functions
	Bpe     float64       // Bits per element

	// Indexes slice will hold the Indexes, representing Bits to be set/read and
	// is under the assumption that it's consumed at only 1 place at a time. Add
	// a lock when multiple clients can be supported.
	Indexes []uint64
}

// NewBloomOpts extracts the user defined values from `args`. It falls back to
// default values if `useDefaults` is set to true. Using those values, it
// creates and returns the options for bloom filter.
func NewBloomOpts(args []string, useDefaults bool) (*BloomOpts, error) {
	if useDefaults {
		return &BloomOpts{ErrorRate: defaultErrorRate, Capacity: defaultCapacity}, nil
	}

	errorRate, err := strconv.ParseFloat(args[0], 64)
	if err != nil {
		return nil, errInvalidErrorRateType
	}

	if errorRate <= 0 || errorRate >= 1.0 {
		return nil, errInvalidErrorRate
	}

	capacity, err := strconv.ParseUint(args[1], 10, 64)
	if err != nil {
		return nil, errInvalidCapacityType
	}

	if capacity < 1 {
		return nil, errInvalidCapacity
	}

	return &BloomOpts{ErrorRate: errorRate, Capacity: capacity}, nil
}

// UpdateIndexes updates the list with Indexes where Bits are supposed to be
// set (to 1) or read in/from the underlying array. It uses the set hash function
// against the given `value` and caps the index with the total number of Bits.
func (opts *BloomOpts) UpdateIndexes(value string) error {
	// Iterate through the hash functions and get Indexes
	for i := 0; i < len(opts.HashFns); i++ {
		fn := opts.HashFns[i]
		fn.Reset()

		if _, err := fn.Write([]byte(value)); err != nil {
			return err
		}

		// Save the index capped by total number of Bits in the underlying array
		opts.Indexes[i] = fn.Sum64() % opts.Bits
	}

	return nil
}
