package hamt_test

import (
	"encoding/hex"
	"github.com/ipld/go-ipld-prime"
	"io"
	"testing"

	// fhamt "github.com/filecoin-project/go-hamt-ipld/v3"
	qt "github.com/frankban/quicktest"
	hamt "github.com/ipld/go-ipld-adl-hamt"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	cbg "github.com/whyrusleeping/cbor-gen"
)

// TODO: do we really need this type?
type cborString []byte

func (c cborString) MarshalCBOR(w io.Writer) error {
	if err := cbg.WriteMajorTypeHeader(w, cbg.MajByteString, uint64(len(c))); err != nil {
		return err
	}
	_, err := w.Write(c)
	return err
}

func TestFilecoinBasic(t *testing.T) {
	_ = cborString("") // don't let the type go unused

	// Below is the code we used with go-hamt-ipld to produce a HAMT encoded
	// with dag-cbor.
	// The version of go-hamt-ipld used was v3.0.0-20201223215115-47873c31a853.
	// We keep the code commented out to not have the go-ipld-adl-hamt
	// module depend on go-hamt-ipld.
	// In the future, we should find a better way, like a nested module or a
	// test case generator.

	// ctx := context.Background()
	// fnode := fhamt.NewNode(nil,
	// 	fhamt.UseTreeBitWidth(5),
	// 	fhamt.UseHashFunction(func(key []byte) []byte {
	// 		hasher := sha256.New()
	// 		hasher.Write(key)
	// 		return hasher.Sum(nil)
	// 	}),
	// )
	// qt.Assert(t, fnode.Set(ctx, "foo", cborString("bar")), qt.IsNil)
	// qt.Assert(t, fnode.MarshalCBOR(buf), qt.IsNil)
	// fenc := buf.Bytes()
	// t.Logf("go-hamt-ipld: %x", fenc)
	fenc, err := hex.DecodeString("82412081818243666f6f43626172")
	qt.Assert(t, err, qt.IsNil)

	builder := hamt.FilecoinV3Prototype{}.NewBuilder()
	assembler, err := builder.BeginMap(0)
	qt.Assert(t, err, qt.IsNil)

	qt.Assert(t, assembler.AssembleKey().AssignString("foo"), qt.IsNil)
	qt.Assert(t, assembler.AssembleValue().AssignBytes([]byte("bar")), qt.IsNil)
	qt.Assert(t, assembler.Finish(), qt.IsNil)

	// TODO: can we do better than these type asserts?
	node := builder.Build().(*hamt.Node)
	enc, err := ipld.Encode(node.Substrate(), dagcbor.Encode)
	qt.Assert(t, err, qt.IsNil)
	t.Logf("go-ipld-adl-hamt: %x", fenc)

	qt.Assert(t, enc, qt.DeepEquals, fenc)
}
