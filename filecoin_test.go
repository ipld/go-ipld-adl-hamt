package hamt_test

import (
	"encoding/hex"
	"io"
	"os"
	"path"
	"runtime"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"

	// fhamt "github.com/filecoin-project/go-hamt-ipld/v3"
	qt "github.com/frankban/quicktest"
	hamt "github.com/ipld/go-ipld-adl-hamt"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/bindnode"
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

func generateFilecoinStateTreeStore() (cid.Cid, linking.LinkSystem) {
	// The fixtures below were generated by running 40 iterations of
	// https://github.com/filecoin-project/specs-actors/blob/d8d9867f68a3c299295efdc6d1b3421c9b63df57/actors/states/tree_test.go#L79-L94
	_, filename, _, _ := runtime.Caller(0)

	files, err := os.ReadDir(path.Join(path.Dir(filename), "fixtures"))
	if err != nil {
		panic(err)
	}

	store := cidlink.Memory{
		Bag: make(map[string][]byte),
	}

	// preload the fixtures in the bag
	for _, file := range files {
		k, err := cid.Decode(file.Name())
		if err != nil {
			panic(err)
		}

		f, err := os.Open(path.Join(path.Dir(filename), "fixtures", file.Name()))
		if err != nil {
			panic(err)
		}
		data, err := io.ReadAll(f)
		if err != nil {
			panic(err)
		}

		store.Bag[string(k.Hash())] = data
	}

	ls := cidlink.DefaultLinkSystem()
	ls.StorageReadOpener = store.OpenRead
	ls.StorageWriteOpener = store.OpenWrite

	// The Map root
	root, err := cid.Decode("bafy2bzaceco2ynykenmzjht6mtmu2enaw5nfaxa5jhtpqjclecb7aqdygzqok")
	if err != nil {
		panic(err)
	}
	return root, ls
}

func TestFilecoinAdtMap(t *testing.T) {
	root, ls := generateFilecoinStateTreeStore()
	ndr, err := ls.Load(ipld.LinkContext{}, cidlink.Link{Cid: root}, hamt.HashMapNodePrototype.Representation())
	if err != nil {
		t.Fatal(err)
	}

	builder := hamt.FilecoinV3Prototype{}.NewBuilder().(*hamt.Builder)
	builder = builder.WithLinking(ls, cidlink.LinkPrototype{})

	nd := builder.Build().(*hamt.Node)

	hmn := bindnode.Unwrap(ndr).(*hamt.HashMapNode)
	if hmn == nil {
		t.Fatal("invalid hashmap node")
	}
	nd.Hamt = *hmn

	// id address #39
	addrKey, err := hex.DecodeString("0027")
	if err != nil {
		t.Fatal(err)
	}

	val, err := nd.LookupByString(string(addrKey))
	if err != nil {
		t.Fatal(err)
	}

	// Actor CallSeqNum
	sn, err := val.LookupByIndex(2)
	if err != nil {
		t.Fatal(err)
	}
	num, err := sn.AsInt()
	if err != nil {
		t.Fatal(err)
	}

	qt.Assert(t, int64(39), qt.Equals, num)
}
