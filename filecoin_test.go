package hamt_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"

	fhamt "github.com/filecoin-project/go-hamt-ipld/v3"
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
	ctx := context.Background()

	buf := new(bytes.Buffer)

	fnode := fhamt.NewNode(nil)
	qt.Assert(t, fnode.Set(ctx, "foo", cborString("bar")), qt.IsNil)
	qt.Assert(t, fnode.MarshalCBOR(buf), qt.IsNil)

	fenc := buf.Bytes()
	fmt.Printf("%q\n", fenc)

	builder := hamt.Prototype{}.NewBuilder()
	assembler, err := builder.BeginMap(0)
	qt.Assert(t, err, qt.IsNil)

	qt.Assert(t, assembler.AssembleKey().AssignString("foo"), qt.IsNil)
	qt.Assert(t, assembler.AssembleValue().AssignString("bar"), qt.IsNil)
	qt.Assert(t, assembler.Finish(), qt.IsNil)

	node := builder.Build()
	buf.Reset()
	qt.Assert(t, dagcbor.Encoder(node, buf), qt.IsNil)
	enc := buf.Bytes()
	fmt.Printf("%q\n", enc)
}
