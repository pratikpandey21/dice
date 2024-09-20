package bloom

import (
	"github.com/dicedb/dice/internal/clientio"
	ds "github.com/dicedb/dice/internal/datastructures"
	diceerrors "github.com/dicedb/dice/internal/errors"
	"github.com/dicedb/dice/internal/ops"
	"github.com/dicedb/dice/internal/store"
)

const (
	defaultExpiry = -1
)

type Eval struct {
	store *store.Store[ds.DSInterface]
	op    *ops.Operation
}

func NewEval(store *store.Store[ds.DSInterface], op *ops.Operation) *Eval {
	return &Eval{
		store: store,
		op:    op,
	}
}

func (e *Eval) Evaluate() []byte {
	switch e.op.Cmd {
	case "BF.ADD":
		// Put the value in the store with the most memory efficient precision type
		return e.BFADD(e.op.Args)
	case "BF.EXISTS":
		return e.BFEXISTS(e.op.Args)
	case "BF.INFO":
		return e.BFINFO(e.op.Args)
	case "BF.INIT":
		return e.BFINIT(e.op.Args)
	}

	return nil
}

func (e *Eval) BFADD(args []string) []byte {
	if len(args) != 2 {
		return diceerrors.NewErrArity("BFADD")
	}

	opts, err := NewBloomOpts(args[1:], true)
	if err != nil {
		return diceerrors.NewErrWithFormattedMessage("%w for 'BFADD' command", err)
	}

	bloom, err := e.getOrCreateBloomFilter(args[0], opts)
	if err != nil {
		return diceerrors.NewErrWithFormattedMessage("%w for 'BFADD' command", err)
	}

	resp, err := bloom.Add(args[1])
	if err != nil {
		return diceerrors.NewErrWithFormattedMessage("%w for 'BFADD' command", err)
	}

	return []byte(string(rune(resp)))
}

func (e *Eval) BFINIT(args []string) []byte {
	if len(args) != 1 && len(args) != 3 {
		return diceerrors.NewErrArity("BFINIT")
	}

	useDefaults := false
	if len(args) == 1 {
		useDefaults = true
	}

	opts, err := NewBloomOpts(args[1:], useDefaults)
	if err != nil {
		return diceerrors.NewErrWithFormattedMessage("%w for 'BFINIT' command", err)
	}

	_, err = e.getOrCreateBloomFilter(args[0], opts)
	if err != nil {
		return diceerrors.NewErrWithFormattedMessage("%w for 'BFINIT' command", err)
	}

	return clientio.RespOK
}

func (e *Eval) BFEXISTS(args []string) []byte {
	if len(args) != 2 {
		return diceerrors.NewErrArity("BFEXISTS")
	}

	bloom, err := e.getOrCreateBloomFilter(args[0], nil)
	if err != nil {
		return diceerrors.NewErrWithFormattedMessage("%w for 'BFEXISTS' command", err)
	}

	resp, err := bloom.Exists(args[1])
	if err != nil {
		return diceerrors.NewErrWithFormattedMessage("%w for 'BFEXISTS' command", err)
	}

	return []byte(string(rune(resp)))
}

func (e *Eval) BFINFO(args []string) []byte {
	if len(args) != 1 {
		return diceerrors.NewErrArity("BFINFO")
	}

	bloom, err := e.getOrCreateBloomFilter(args[0], nil)
	if err != nil {
		return diceerrors.NewErrWithFormattedMessage("%w for 'BFINFO' command", err)
	}

	return []byte(bloom.Info(args[0]))
}


func (e *Eval) getOrCreateBloomFilter(key string, opts *BloomOpts) (*Bloom, error) {
	obj, exists := e.store.Get(key)

	// If we don't have a filter yet and `opts` are provided, create one.
	if !exists && opts != nil {
		obj = NewBloomFilter(opts)
		e.store.Put(key, obj, defaultExpiry)
	}

	// If no `opts` are provided for filter creation, return err
	if !exists && opts == nil {
		return nil, errInvalidKey
	}

	return obj.(*Bloom), nil
}
