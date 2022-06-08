package hamt

import (
	_ "embed"
	"fmt"

	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/node/bindnode"
	"github.com/ipld/go-ipld-prime/schema"
	"github.com/multiformats/go-multicodec"
)

var (
	//go:embed schema.ipldsch
	schemaBytes          []byte
	HashMapNodePrototype schema.TypedPrototype
	HashMapRootPrototype schema.TypedPrototype
)

type (
	HashMapRoot struct {
		HashAlg    multicodec.Code
		BucketSize int
		Hamt       HashMapNode
	}
	HashMapNode struct {
		Map  []byte
		Data []Element
	}
	Element struct {
		HashMapNode *ipld.Link
		Bucket      *Bucket
	}
	Bucket      []BucketEntry
	BucketEntry struct {
		Key   []byte
		Value ipld.Node
	}
)

func init() {
	ts, err := ipld.LoadSchemaBytes(schemaBytes)
	if err != nil {
		panic(fmt.Errorf("failed to load schema: %w", err))
	}
	HashMapRootPrototype = bindnode.Prototype((*HashMapRoot)(nil), ts.TypeByName("HashMapRoot"))
	HashMapNodePrototype = bindnode.Prototype((*HashMapNode)(nil), ts.TypeByName("HashMapNode"))
}
