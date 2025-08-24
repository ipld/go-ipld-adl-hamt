package hamt

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/google/go-cmp/cmp"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	_ "github.com/ipld/go-ipld-prime/codec/dagcbor"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/storage/memstore"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
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

	sampleCid, err := cid.Cast([]byte{1, 85, 0, 5, 0, 1, 2, 3, 4})
	qt.Assert(t, err, qt.IsNil)

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
		{"AssignLink", cidlink.Link{Cid: sampleCid}},

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
			case ipld.Link:
				err = assembler.AssembleValue().AssignLink(value)
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
			qt.Assert(t, val,
				// To support comparing cid.Cid.
				qt.CmpEquals(cmp.Comparer(func(c1, c2 cid.Cid) bool {
					return c1.Equals(c2)
				})),
				test.value)
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
	case ipld.Kind_Link:
		val, err = node.AsLink()
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

	linkSystem := cidlink.DefaultLinkSystem()

	// TODO: surely Version and MhLength could be inferred?
	linkProto := cidlink.LinkPrototype{Prefix: cid.Prefix{
		Version:  1, // Usually '1'.
		Codec:    uint64(multicodec.DagCbor),
		MhType:   uint64(multicodec.Sha3_384),
		MhLength: 48, // sha3-384 hash has a 48-byte sum.
	}}

	storage := make(map[ipld.Link][]byte)
	linkSystem.StorageWriteOpener = func(lctx ipld.LinkContext) (io.Writer, ipld.BlockWriteCommitter, error) {
		buf := bytes.Buffer{}
		return &buf, func(lnk ipld.Link) error {
			storage[lnk] = buf.Bytes()
			return nil
		}, nil

	}
	linkSystem.StorageReadOpener = func(_ ipld.LinkContext, lnk ipld.Link) (io.Reader, error) {
		return bytes.NewReader(storage[lnk]), nil
	}

	builder := NewBuilder(Prototype{BitWidth: 3, BucketSize: 2}).
		WithLinking(linkSystem, linkProto)

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

func TestIterator_DoneDoesNotUpdateState(t *testing.T) {
	t.Parallel()

	builder := Prototype{}.NewBuilder()
	assembler, err := builder.BeginMap(0)
	qt.Assert(t, err, qt.IsNil)

	const entryCount = 2
	wantMap := make(map[string]string)
	for i := 0; i < entryCount; i++ {
		key := fmt.Sprintf("key-%02d", i)
		value := fmt.Sprintf("value-%02d", i)
		qt.Assert(t, assembler.AssembleKey().AssignString(key), qt.IsNil)
		qt.Assert(t, assembler.AssembleValue().AssignString(value), qt.IsNil)
		wantMap[key] = value
	}
	qt.Assert(t, assembler.Finish(), qt.IsNil)

	node := builder.Build()
	qt.Assert(t, node.Length(), qt.Equals, int64(entryCount))

	iter := node.MapIterator()
	// Assert repeated Done calls do not impact position of the iterator.
	qt.Assert(t, iter.Done(), qt.IsFalse)
	qt.Assert(t, iter.Done(), qt.IsFalse)
	qt.Assert(t, iter.Done(), qt.IsFalse)
	qt.Assert(t, iter.Done(), qt.IsFalse)

	// Assert Next() element is as expected.
	k1, v1, err := iter.Next()
	qt.Assert(t, err, qt.IsNil)
	gotK1, err := k1.AsString()
	qt.Assert(t, err, qt.IsNil)
	gotV1, err := v1.AsString()
	qt.Assert(t, err, qt.IsNil)
	wantV1, ok := wantMap[gotK1]
	qt.Assert(t, ok, qt.IsTrue)
	qt.Assert(t, gotV1, qt.Equals, wantV1)

	// Without calling Done(), assert Next() element is as expected.
	k2, v2, err := iter.Next()
	qt.Assert(t, err, qt.IsNil)
	gotK2, err := k2.AsString()
	qt.Assert(t, err, qt.IsNil)
	gotV2, err := v2.AsString()
	qt.Assert(t, err, qt.IsNil)
	wantV2, ok := wantMap[gotK2]
	qt.Assert(t, ok, qt.IsTrue)
	qt.Assert(t, gotV2, qt.Equals, wantV2)

	// There should only be 2 entries in the map; assert that the iterator is done.
	qt.Assert(t, iter.Done(), qt.IsTrue)

	// Assert that the iterator remains done.
	qt.Assert(t, iter.Done(), qt.IsTrue)
	qt.Assert(t, iter.Done(), qt.IsTrue)
	qt.Assert(t, iter.Done(), qt.IsTrue)

	// Assert that calling next is error
	k3, v3, err := iter.Next()
	qt.Assert(t, k3, qt.IsNil)
	qt.Assert(t, v3, qt.IsNil)
	qt.Assert(t, err, qt.Equals, ipld.ErrIteratorOverread{})
}

func TestIteratorWithLinks(t *testing.T) {
	t.Parallel()

	ls := cidlink.DefaultLinkSystem()
	store := &memstore.Store{}
	ls.SetWriteStorage(store)
	ls.SetReadStorage(store)

	lp := cidlink.LinkPrototype{
		Prefix: cid.Prefix{
			Version:  1,
			Codec:    cid.DagCBOR,
			MhType:   multihash.SHA2_256,
			MhLength: -1,
		},
	}

	builder := NewBuilder(Prototype{}).WithLinking(ls, lp)
	assembler, err := builder.BeginMap(0)
	qt.Assert(t, err, qt.IsNil)

	// Pick a sufficiently large number so that we end up with links and buckets.
	const wantEntryCount = int64(500)

	wantEntries := make(map[string]string)
	for i := int64(0); i < wantEntryCount; i++ {
		key := fmt.Sprintf("key-%02d", i)
		value := fmt.Sprintf("value-%02d", i)
		qt.Assert(t, assembler.AssembleKey().AssignString(key), qt.IsNil)
		qt.Assert(t, assembler.AssembleValue().AssignString(value), qt.IsNil)
		wantEntries[key] = value
	}
	qt.Assert(t, assembler.Finish(), qt.IsNil)

	node := builder.Build()

	qt.Assert(t, node.Length(), qt.Equals, wantEntryCount)

	gotEntryCount := int64(0)
	iter := node.MapIterator()
	for !iter.Done() {
		gotKn, gotVn, err := iter.Next()
		qt.Assert(t, err, qt.IsNil)
		if gotEntryCount++; gotEntryCount > wantEntryCount {
			t.Logf("stopping iteration since found more than the expected entry count")
			break
		}
		key := basicValue(t, gotKn).(string)

		wantVal, ok := wantEntries[key]
		qt.Assert(t, ok, qt.IsTrue)

		gotVal := basicValue(t, gotVn)
		qt.Assert(t, gotVal, qt.Equals, wantVal)

		// Delete so that we can assert the map iterator returns each key exactly once.
		delete(wantEntries, key)
	}
	qt.Assert(t, len(wantEntries), qt.Equals, 0)
	qt.Assert(t, gotEntryCount, qt.Equals, wantEntryCount)
}

func TestBuilder(t *testing.T) {
	t.Parallel()

	builder := Prototype{}.NewBuilder()
	asm1, err := builder.BeginMap(0)
	qt.Assert(t, err, qt.IsNil)

	qt.Assert(t, asm1.AssembleKey().AssignString("foo"), qt.IsNil)
	qt.Assert(t, asm1.AssembleValue().AssignString("x"), qt.IsNil)
	qt.Assert(t, asm1.Finish(), qt.IsNil)

	node1 := builder.Build()

	qt.Assert(t, node1.Length(), qt.Equals, int64(1))

	// If we reset the builder, the node should still be the same.
	builder.Reset()
	qt.Assert(t, node1.Length(), qt.Equals, int64(1))

	// If we build a new node, both nodes should work as expected.
	asm2, err := builder.BeginMap(0)
	qt.Assert(t, err, qt.IsNil)

	qt.Assert(t, asm2.AssembleKey().AssignString("foo"), qt.IsNil)
	qt.Assert(t, asm2.AssembleValue().AssignString("x"), qt.IsNil)
	qt.Assert(t, asm2.AssembleKey().AssignString("bar"), qt.IsNil)
	qt.Assert(t, asm2.AssembleValue().AssignString("y"), qt.IsNil)
	qt.Assert(t, asm2.Finish(), qt.IsNil)

	node2 := builder.Build()

	qt.Assert(t, node1.Length(), qt.Equals, int64(1))
	qt.Assert(t, node2.Length(), qt.Equals, int64(2))
}

func TestCOW(t *testing.T) {
	t.Parallel()

	builder := Prototype{}.NewBuilder()
	assembler, err := builder.BeginMap(0)
	qt.Assert(t, err, qt.IsNil)

	qt.Assert(t, assembler.AssembleKey().AssignString("foo"), qt.IsNil)
	qt.Assert(t, assembler.AssembleValue().AssignString("bar1"), qt.IsNil)
	qt.Assert(t, assembler.Finish(), qt.IsNil)

	node := builder.Build()

	qt.Assert(t, node.Length(), qt.Equals, int64(1))

	val, err := node.LookupByString("foo")
	qt.Assert(t, err, qt.IsNil)
	valStr, err := val.AsString()
	qt.Assert(t, err, qt.IsNil)
	qt.Assert(t, valStr, qt.Equals, "bar1")

	builder.Reset()
	builder.(*Builder).StartWith(node)
	qt.Assert(t, assembler.AssembleKey().AssignString("foo2"), qt.IsNil)
	qt.Assert(t, assembler.AssembleValue().AssignString("bar2"), qt.IsNil)
	qt.Assert(t, assembler.Finish(), qt.IsNil)

	node2 := builder.Build()

	qt.Check(t, node2.Length(), qt.Equals, int64(2))

	val, err = node2.LookupByString("foo")
	qt.Assert(t, err, qt.IsNil)
	valStr, err = val.AsString()
	qt.Assert(t, err, qt.IsNil)
	qt.Assert(t, valStr, qt.Equals, "bar1")
	val, err = node2.LookupByString("foo2")
	qt.Assert(t, err, qt.IsNil)
	valStr, err = val.AsString()
	qt.Assert(t, err, qt.IsNil)
	qt.Assert(t, valStr, qt.Equals, "bar2")
}
