package bytearray

import (
	ds "github.com/dicedb/dice/internal/datastructures"
)

type ByteArray struct {
	ds.BaseDataStructure[ds.DSInterface]
	Bytes  []byte
	length int64
}

var (
	_ ds.DSInterface = &ByteArray{}
)

func NewByteArray(size int) *ByteArray {
	return &ByteArray{
		Bytes:  make([]byte, size),
		length: int64(size),
	}
}
