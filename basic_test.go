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

	qt.Assert(t, node.Length(), qt.Equals, int64(1))

	valNode, err := node.LookupByString("foo")
	qt.Assert(t, err, qt.IsNil)
	val := basicValue(t, valNode)
	qt.Assert(t, val, qt.Equals, "bar")
}

func TestTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value interface{}
	}{
		// {"AssignNull", nil},
		{"AssignBool", true},
		{"AssignInt", int64(3)},
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
			case int64:
				err = assembler.AssembleValue().AssignInt(value)
			case float64:
				err = assembler.AssembleValue().AssignFloat(value)
			case string:
				err = assembler.AssembleValue().AssignString(value)
			case []byte:
				err = assembler.AssembleValue().AssignBytes(value)
			default:
				t.Fatalf("unexpected value type: %T\n", value)
			}
			qt.Assert(t, err, qt.IsNil)
			err = assembler.Finish()
			qt.Assert(t, err, qt.IsNil)

			node := builder.Build()

			qt.Assert(t, node.Length(), qt.Equals, int64(1))

			valNode, err := node.LookupByString(key)
			qt.Assert(t, err, qt.IsNil)

			val := basicValue(t, valNode)
			qt.Assert(t, val, qt.DeepEquals, test.value)
		})
	}
}

func basicValue(t *testing.T, node ipld.Node) interface{} {
	if node == nil {
		t.Fatalf("unexpected nil ipld.Node")
	}
	var val interface{}
	var err error
	switch kind := node.Kind(); kind {
	case ipld.Kind_Null:
		return nil
	case ipld.Kind_Bool:
		val, err = node.AsBool()
	case ipld.Kind_Int:
		val, err = node.AsInt()
	case ipld.Kind_Float:
		val, err = node.AsFloat()
	case ipld.Kind_String:
		val, err = node.AsString()
	case ipld.Kind_Bytes:
		val, err = node.AsBytes()
	default:
		t.Fatalf("node does not have a basic kind: %v\n", kind)
	}
	qt.Assert(t, err, qt.IsNil)
	return val
}

func TestLargeBuckets(t *testing.T) {
	t.Parallel()

	builder := Prototype{BitWidth: 3, BucketSize: 64}.NewBuilder()
	assembler, err := builder.BeginMap(0)
	qt.Assert(t, err, qt.IsNil)

	const number = int64(100)
	for i := int64(0); i < number; i++ {
		s := fmt.Sprintf("%02d", i)
		qt.Assert(t, assembler.AssembleKey().AssignString(s), qt.IsNil)
		qt.Assert(t, assembler.AssembleValue().AssignString(s), qt.IsNil)
	}
	qt.Assert(t, assembler.Finish(), qt.IsNil)

	node := builder.Build()

	qt.Assert(t, node.Length(), qt.Equals, number)

	for i := int64(0); i < number; i++ {
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

	qt.Assert(t, node.Length(), qt.Equals, int64(1))

	val, err := node.LookupByString("foo")
	qt.Assert(t, err, qt.IsNil)
	valStr, err := val.AsString()
	qt.Assert(t, err, qt.IsNil)
	qt.Assert(t, valStr, qt.Equals, "bar2")
}

func TestLinks(t *testing.T) {
	t.Parallel()

	// TODO: surely Version and MhLength could be inferred?
	linkBuilder := cidlink.LinkBuilder{Prefix: cid.Prefix{
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

	const number = int64(20)
	for i := int64(0); i < number; i++ {
		s := fmt.Sprintf("%02d", i)
		qt.Assert(t, assembler.AssembleKey().AssignString(s), qt.IsNil)
		qt.Assert(t, assembler.AssembleValue().AssignString(s), qt.IsNil)
	}
	qt.Assert(t, assembler.Finish(), qt.IsNil)
	node := Build(builder)

	qt.Assert(t, node.Length(), qt.Equals, number)

	for i := int64(0); i < number; i++ {
		s := fmt.Sprintf("%02d", i)
		val, err := node.LookupByString(s)
		qt.Assert(t, err, qt.IsNil)
		valStr, err := val.AsString()
		qt.Assert(t, err, qt.IsNil)
		qt.Assert(t, valStr, qt.Equals, s)
	}
}

func TestIterator(t *testing.T) {
	t.Parallel()

	builder := Prototype{}.NewBuilder()
	assembler, err := builder.BeginMap(0)
	qt.Assert(t, err, qt.IsNil)

	const number = int64(10)
	seen := make(map[string]int)
	for i := int64(0); i < number; i++ {
		key := fmt.Sprintf("key-%02d", i)
		value := fmt.Sprintf("value-%02d", i)
		qt.Assert(t, assembler.AssembleKey().AssignString(key), qt.IsNil)
		qt.Assert(t, assembler.AssembleValue().AssignString(value), qt.IsNil)
		seen[key] = 1
	}
	qt.Assert(t, assembler.Finish(), qt.IsNil)

	node := builder.Build()

	qt.Assert(t, node.Length(), qt.Equals, number)

	gotNumber := int64(0)
	iter := node.MapIterator()
	for !iter.Done() {
		keyNode, _, err := iter.Next()
		qt.Assert(t, err, qt.IsNil)
		if gotNumber++; gotNumber >= number*2 {
			t.Logf("stopping iteration as it looks like an endless loop")
			break
		}
		key := basicValue(t, keyNode).(string)
		if seen[key] == 0 {
			t.Fatalf("unexpected key: %q", key)
		}
		if seen[key] > 1 {
			t.Fatalf("duplicate key: %q", key)
		}
		seen[key] = seen[key] + 1
	}
	qt.Assert(t, gotNumber, qt.Equals, number)
}
