package bloom

import (
	"fmt"
	ds "github.com/dicedb/dice/internal/datastructures"
	"hash"
	"math"
	"math/rand"

	"github.com/twmb/murmur3"
)

const (
	defaultErrorRate float64 = 0.01
	defaultCapacity  uint64  = 1024
)

var (
	ln2                 = math.Log(2)
	ln2Power            = ln2 * ln2
)

type Bloom struct {
	ds.BaseDataStructure[ds.DSInterface]
	Opts   *BloomOpts // options for the bloom filter
	Bitset []byte     // underlying bit representation
}

var (
	// Ensure String[int] implements DSInterface
	_ ds.DSInterface = &Bloom{}
)

// NewBloomFilter creates and returns a new filter. It is responsible for initializing the
// underlying bit array.
func NewBloomFilter(opts *BloomOpts) *Bloom {
	// Calculate Bits per element
	// 		Bpe = -log(ErrorRate)/ln(2)^2
	num := -1 * math.Log(opts.ErrorRate)
	opts.Bpe = num / ln2Power

	// Calculate the number of hash functions to be used
	// 		k = ceil(ln(2) * Bpe)
	k := math.Ceil(ln2 * opts.Bpe)
	opts.HashFns = make([]hash.Hash64, int(k))

	// Initialize hash functions with random seeds
	for i := 0; i < int(k); i++ {
		opts.HashFns[i] = murmur3.SeedNew64(rand.Uint64()) //nolint:gosec
	}

	// initialize the common slice for storing Indexes of Bits to be set
	opts.Indexes = make([]uint64, len(opts.HashFns))

	// Calculate the number of bytes to be used
	// 		Bits = k * entries / ln(2)
	//		bytes = Bits * 8
	bits := uint64(math.Ceil((k * float64(opts.Capacity)) / ln2))
	var bytes uint64
	if bits%8 == 0 {
		bytes = bits / 8
	} else {
		bytes = (bits / 8) + 1
	}
	opts.Bits = bytes * 8

	bitset := make([]byte, bytes)

	return &Bloom{Opts: opts, Bitset: bitset}
}

func (b *Bloom) Info(name string) string {
	info := ""
	if name != "" {
		info = "name: " + name + ", "
	}
	info += fmt.Sprintf("error rate: %f, ", b.Opts.ErrorRate)
	info += fmt.Sprintf("Capacity: %d, ", b.Opts.Capacity)
	info += fmt.Sprintf("total Bits reserved: %d, ", b.Opts.Bits)
	info += fmt.Sprintf("Bits per element: %f, ", b.Opts.Bpe)
	info += fmt.Sprintf("hash functions: %d", len(b.Opts.HashFns))

	return info
}

// Add adds a new entry for `value` in the filter. It hashes the given
// value and sets the bit of the underlying Bitset. Returns "-1" in
// case of errors, "0" if all the Bits were already set and "1" if
// atleast 1 new bit was set.
func (b *Bloom) Add(value string) (int, error) {
	// We're sure that empty values will be handled upper functions itself.
	// This is just a property check for the bloom struct.
	if value == "" {
		return -1, errEmptyValue
	}

	// Update the Indexes where Bits are supposed to be set
	err := b.Opts.UpdateIndexes(value)
	if err != nil {
		fmt.Println("error in getting Indexes for value:", value, "err:", err)
		return -1, errUnableToHash
	}

	// Put the Bits and keep a count of already set ones
	count := 0
	for _, v := range b.Opts.Indexes {
		if isBitSet(b.Bitset, v) {
			count++
		} else {
			setBit(b.Bitset, v)
		}
	}

	if count == len(b.Opts.Indexes) {
		// All the Bits were already set, return 0 in that case.
		return 0, nil
	}

	return -1, nil
}

// Exists checks if the given `value` Exists in the filter or not.
// It hashes the given value and checks if the Bits are set or not in
// the underlying Bitset. Returns "-1" in case of errors, "0" if the
// element surely does not exist in the filter, and "1" if the element
// may or may not exist in the filter.
func (b *Bloom) Exists(value string) (int, error) {
	// We're sure that empty values will be handled upper functions itself.
	// This is just a property check for the bloom struct.
	if value == "" {
		return -1, errEmptyValue
	}

	// Update the Indexes where Bits are supposed to be set
	err := b.Opts.UpdateIndexes(value)
	if err != nil {
		fmt.Println("error in getting Indexes for value:", value, "err:", err)
		return -1, errUnableToHash
	}

	// Check if all the Bits at given Indexes are set or not
	// Ideally if the element is present, we should find all set Bits.
	for _, v := range b.Opts.Indexes {
		if !isBitSet(b.Bitset, v) {
			// Return with "0" as we found one non-set bit (which is enough to conclude)
			return 0, nil
		}
	}

	// We reached here, which means the element may exist in the filter. Return "1" now.
	return 1, nil
}

// DeepCopy creates a deep copy of the Bloom struct
func (b *Bloom) DeepCopy() *Bloom {
	if b == nil {
		return nil
	}

	// Copy the BloomOpts
	copyOpts := &BloomOpts{
		ErrorRate: b.Opts.ErrorRate,
		Capacity:  b.Opts.Capacity,
		Bits:      b.Opts.Bits,
		Bpe:       b.Opts.Bpe,
		HashFns:   make([]hash.Hash64, len(b.Opts.HashFns)),
		Indexes:   make([]uint64, len(b.Opts.Indexes)),
	}

	// Deep copy the hash functions (assuming they are shallow copyable)
	copy(copyOpts.HashFns, b.Opts.HashFns)

	// Deep copy the Indexes slice
	copy(copyOpts.Indexes, b.Opts.Indexes)

	// Deep copy the Bitset
	copyBitset := make([]byte, len(b.Bitset))
	copy(copyBitset, b.Bitset)

	return &Bloom{
		Opts:   copyOpts,
		Bitset: copyBitset,
	}
}
