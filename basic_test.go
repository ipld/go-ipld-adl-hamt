package hamt

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	_ "github.com/ipld/go-ipld-prime/codec/dagcbor"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/multiformats/go-multicodec"
)

func TestBasic(t *testing.T) {
	t.Parallel()

	builder := Prototype{}.NewBuilder()
	assembler, err := builder.BeginMap(0)
	qt.Assert(t, err, qt.IsNil)

	qt.Assert(t, assembler.AssembleKey().AssignString("foo"), qt.IsNil)
	qt.Assert(t, assembler.AssembleValue().AssignString("bar"), qt.IsNil)
	qt.Assert(t, assembler.Finish(), qt.IsNil)

	node := builder.Build()

	qt.Assert(t, node.Length(), qt.Equals, 1)

	val, err := node.LookupByString("foo")
	qt.Assert(t, err, qt.IsNil)
	valStr, err := val.AsString()
	qt.Assert(t, err, qt.IsNil)
	qt.Assert(t, valStr, qt.Equals, "bar")
}

func TestTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value interface{}
	}{
		// {"AssignNull", nil},
		{"AssignBool", true},
		{"AssignInt", 3},
		{"AssignFloat", 4.5},
		{"AssignString", "foo"},
		{"AssignBytes", []byte{1, 2, 3}},
		// TODO: AssignLink
		// TODO: AssignNode
	}
	for i, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			i := i
			test := test
			t.Parallel()

			builder := Prototype{}.NewBuilder()
			assembler, err := builder.BeginMap(0)
			qt.Assert(t, err, qt.IsNil)

			key := fmt.Sprintf("%d", i)
			qt.Assert(t, assembler.AssembleKey().AssignString(key), qt.IsNil)
			switch value := test.value.(type) {
			case nil:
				err = assembler.AssembleValue().AssignNull()
			case bool:
				err = assembler.AssembleValue().AssignBool(value)
			case int:
				err = assembler.AssembleValue().AssignInt(value)
			case float64:
				err = assembler.AssembleValue().AssignFloat(value)
			case string:
				err = assembler.AssembleValue().AssignString(value)
			case []byte:
				err = assembler.AssembleValue().AssignBytes(value)
			default:
				t.Fatalf("unexpected value type: %#v\n", value)
			}
			qt.Assert(t, err, qt.IsNil)
			err = assembler.Finish()
			qt.Assert(t, err, qt.IsNil)

			node := builder.Build()

			qt.Assert(t, node.Length(), qt.Equals, 1)

			valNode, err := node.LookupByString(key)
			qt.Assert(t, err, qt.IsNil)

			var val interface{}
			switch value := test.value.(type) {
			case nil:
				qt.Assert(t, valNode.IsNull(), qt.IsTrue)
			case bool:
				val, err = valNode.AsBool()
			case int:
				val, err = valNode.AsInt()
			case float64:
				val, err = valNode.AsFloat()
			case string:
				val, err = valNode.AsString()
			case []byte:
				val, err = valNode.AsBytes()
			default:
				t.Fatalf("unexpected value type: %#v\n", value)
			}

			qt.Assert(t, err, qt.IsNil)
			qt.Assert(t, val, qt.DeepEquals, test.value)
		})
	}
}

func TestLargeBuckets(t *testing.T) {
	t.Parallel()

	builder := Prototype{BitWidth: 3, BucketSize: 64}.NewBuilder()
	assembler, err := builder.BeginMap(0)
	qt.Assert(t, err, qt.IsNil)

	const number = 100
	for i := 0; i < number; i++ {
		s := fmt.Sprintf("%02d", i)
		qt.Assert(t, assembler.AssembleKey().AssignString(s), qt.IsNil)
		qt.Assert(t, assembler.AssembleValue().AssignString(s), qt.IsNil)
	}
	qt.Assert(t, assembler.Finish(), qt.IsNil)

	node := builder.Build()

	qt.Assert(t, node.Length(), qt.Equals, number)

	for i := 0; i < number; i++ {
		s := fmt.Sprintf("%02d", i)
		val, err := node.LookupByString(s)
		qt.Assert(t, err, qt.IsNil)
		valStr, err := val.AsString()
		qt.Assert(t, err, qt.IsNil)
		qt.Assert(t, valStr, qt.Equals, s)
	}
}

func TestReplace(t *testing.T) {
	t.Parallel()

	builder := Prototype{}.NewBuilder()
	assembler, err := builder.BeginMap(0)
	qt.Assert(t, err, qt.IsNil)

	qt.Assert(t, assembler.AssembleKey().AssignString("foo"), qt.IsNil)
	qt.Assert(t, assembler.AssembleValue().AssignString("bar1"), qt.IsNil)
	qt.Assert(t, assembler.AssembleKey().AssignString("foo"), qt.IsNil)
	qt.Assert(t, assembler.AssembleValue().AssignString("bar2"), qt.IsNil)
	qt.Assert(t, assembler.Finish(), qt.IsNil)

	node := builder.Build()

	qt.Assert(t, node.Length(), qt.Equals, 1)

	val, err := node.LookupByString("foo")
	qt.Assert(t, err, qt.IsNil)
	valStr, err := val.AsString()
	qt.Assert(t, err, qt.IsNil)
	qt.Assert(t, valStr, qt.Equals, "bar2")
}

func TestLinks(t *testing.T) {
	t.Parallel()

	// TODO: surely Version and MhLength could be inferred?
	linkBuilder := cidlink.LinkBuilder{cid.Prefix{
		Version:  1, // Usually '1'.
		Codec:    uint64(multicodec.DagCbor),
		MhType:   uint64(multicodec.Sha3_384),
		MhLength: 48, // sha3-384 hash has a 48-byte sum.
	}}

	storage := make(map[ipld.Link][]byte)
	storer := func(ipld.LinkContext) (io.Writer, ipld.StoreCommitter, error) {
		buf := bytes.Buffer{}
		return &buf, func(lnk ipld.Link) error {
			storage[lnk] = buf.Bytes()
			return nil
		}, nil
	}
	loader := func(lnk ipld.Link, _ ipld.LinkContext) (io.Reader, error) {
		return bytes.NewReader(storage[lnk]), nil
	}

	builder := NewBuilder(Prototype{BitWidth: 3, BucketSize: 2}).
		WithLinking(linkBuilder, loader, storer)

	assembler, err := builder.BeginMap(0)
	qt.Assert(t, err, qt.IsNil)

	const number = 20
	for i := 0; i < number; i++ {
		s := fmt.Sprintf("%02d", i)
		qt.Assert(t, assembler.AssembleKey().AssignString(s), qt.IsNil)
		qt.Assert(t, assembler.AssembleValue().AssignString(s), qt.IsNil)
	}
	qt.Assert(t, assembler.Finish(), qt.IsNil)
	node := Build(builder)

	qt.Assert(t, node.Length(), qt.Equals, number)

	for i := 0; i < number; i++ {
		s := fmt.Sprintf("%02d", i)
		val, err := node.LookupByString(s)
		qt.Assert(t, err, qt.IsNil)
		valStr, err := val.AsString()
		qt.Assert(t, err, qt.IsNil)
		qt.Assert(t, valStr, qt.Equals, s)
	}
}
