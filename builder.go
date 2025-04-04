package hamt

import (
	"fmt"

	"github.com/ipld/go-ipld-prime"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/ipld/go-ipld-prime/node/mixins"
	"github.com/multiformats/go-multicodec"
)

var _ ipld.NodePrototype = (*Prototype)(nil)

type Prototype struct {
	BitWidth   int
	BucketSize int

	// hashAlg requires an extra bool, because the zero value can't be used
	// as the default behavior, since the code 0x00 is a valid multicodec
	// code.
	hashAlg    multicodec.Code
	hashAlgSet bool

	modeFilecoin bool
}

func (p Prototype) WithHashAlg(code multicodec.Code) Prototype {
	p.hashAlg = code
	p.hashAlgSet = true
	return p
}

func (p Prototype) NewBuilder() ipld.NodeBuilder {
	return NewBuilder(p)
}

var _ ipld.NodePrototype = (*FilecoinV3Prototype)(nil)

type FilecoinV3Prototype struct{}

func (p FilecoinV3Prototype) NewBuilder() ipld.NodeBuilder {
	return NewBuilder(Prototype{modeFilecoin: true})
}

var _ ipld.NodeBuilder = (*Builder)(nil)

type Builder struct {
	bitWidth   int
	hashAlg    multicodec.Code
	bucketSize int

	node *Node
}

func NewBuilder(proto Prototype) *Builder {
	if proto.modeFilecoin {
		return &Builder{node: &Node{modeFilecoin: true}}
	}
	// Set the defaults.
	// Following js-ipld-hamt and Filecoin's experience and research,
	// it seems like a bitwidth of 5 and a bucket size of 3 scale well
	// enough for large maps without causing too much churn on edits.
	if proto.BitWidth < 1 {
		proto.BitWidth = 5
	}
	if proto.BucketSize < 1 {
		proto.BucketSize = 3
	}
	if !proto.hashAlgSet {
		proto.hashAlg = multicodec.Murmur3X64_64
	}

	return &Builder{
		bitWidth:   proto.BitWidth,
		hashAlg:    proto.hashAlg,
		bucketSize: proto.BucketSize,

		node: &Node{},
	}
}

func (b Builder) WithLinking(system ipld.LinkSystem, proto ipld.LinkPrototype) *Builder {
	b.node = b.node.WithLinking(system, proto)
	return &b
}

func Build(builder *Builder) *Node {
	return builder.node
}

func (b *Builder) Build() ipld.Node { return Build(b) }
func (b *Builder) Reset()           { b.node = &Node{} }

func (b *Builder) BeginMap(sizeHint int64) (ipld.MapAssembler, error) {
	if b.node.modeFilecoin {
		return &assembler{node: b.node}, nil
	}
	if b.bitWidth < 3 {
		return nil, fmt.Errorf("bitWidth must be at least 3")
	}
	switch b.hashAlg {
	case multicodec.Identity, multicodec.Sha2_256, multicodec.Murmur3X64_64:
	default:
		return nil, fmt.Errorf("unsupported hash algorithm: %x", b.hashAlg)
	}
	b.node.HashMapRoot = HashMapRoot{
		HashAlg:    b.hashAlg,
		BucketSize: b.bucketSize,
		Hamt: HashMapNode{
			Map: make([]byte, 1<<(b.bitWidth-3)),
		},
	}
	return &assembler{node: b.node}, nil
}

// StartWith is an extension to a NodeBuilder which advertises that it can make
// new data structures with copy-on-write internal memory.
//
// It's particularly impactful for something like this HAMT ADL,
// since it means an appended data structure can be made without lots of copying,
// and also no need to re-compute the sharding, bucketing, etc for all that data.
func (b *Builder) StartWith(n ipld.Node) (ipld.MapAssembler, error)  {
	// Of course, we can only really do this well if the node you give us is the type we know well.
	if n2, familiar := n.(*Node); familiar {
		if b.bitWidth != n2.bitWidth() {
			panic(fmt.Errorf("todo: fallback to a copy?  distinct bitwidth; %v %v", b.bitWidth, n2.bitWidth()))
		}
		//if b.hashAlg != n2.hashAlg() { // FIXME oh dear.  This is not recalled and we cannot check it.  (It would be an impossible claim to verify for something serial, anyway, I suppose.)  That... provokes interesting conversations.
		//	panic("todo: fallback to a copy?")
		//}
		if b.bucketSize != n2._bucketSize() {
			panic(fmt.Errorf("todo: fallback to a copy?  distinct bucket size; %v %v", b.bucketSize, n2._bucketSize()))
		}
		// It's also particularly easy to implement this confidently since the internal data structures are immutable in memory, too.
		// TODO ^ check if this claim is actually true.  I am not sure it is.  I think I see touching of the innards.
		// We already have a new Node allocated (we assume) by either NewBuilder or by Reset; we'll just smash the memory of the node we're starting with on top of that, and then we go from there.3
		*b.node = *n2
		return &assembler{node: b.node}, nil
	}
	panic("todo: fallback to a copy?  not this adl impl at all") // REVIEW what the contract of this should be.  I dislike things that can be "suprise!  slow because of argument misalignment we didn't raise to you".
	// I suppose if we have a well-typed error for this, we can make the decision about whether to fall back to a dumb copy or not something that the top level IPLD functions can choose (...assuming that we also pursue that thought).
}
func (b *Builder) BeginList(sizeHint int64) (ipld.ListAssembler, error) { panic("todo: error?") }
func (b *Builder) AssignNull() error                                    { panic("todo: error?") }
func (b *Builder) AssignBool(bool) error                                { panic("todo: error?") }
func (b *Builder) AssignInt(int64) error                                { panic("todo: error?") }
func (b *Builder) AssignFloat(float64) error                            { panic("todo: error?") }
func (b *Builder) AssignString(string) error                            { panic("todo: error?") }
func (b *Builder) AssignBytes([]byte) error                             { panic("todo: error?") }
func (b *Builder) AssignLink(ipld.Link) error                           { panic("todo: error?") }
func (b *Builder) AssignNode(ipld.Node) error                           { panic("todo: error?") }
func (b *Builder) Prototype() ipld.NodePrototype                        { panic("todo: error?") }

type assembler struct {
	node *Node

	assemblingKey []byte
}

func (a *assembler) AssembleKey() ipld.NodeAssembler {
	return keyAssembler{a}
}

func (a *assembler) AssembleValue() ipld.NodeAssembler {
	return valueAssembler{a}
}

func (a *assembler) AssembleEntry(k string) (ipld.NodeAssembler, error) {
	return nil, nil
}

func (a *assembler) Finish() error {
	return nil
}

func (a *assembler) KeyPrototype() ipld.NodePrototype {
	return basicnode.Prototype.Bytes
}

func (a *assembler) ValuePrototype(k string) ipld.NodePrototype {
	return basicnode.Prototype.Any
}

type keyAssembler struct {
	parent *assembler
}

func (keyAssembler) BeginMap(sizeHint int64) (ipld.MapAssembler, error) {
	return mixins.BytesAssembler{TypeName: "bytes"}.BeginMap(0)
}

func (keyAssembler) BeginList(sizeHint int64) (ipld.ListAssembler, error) {
	return mixins.BytesAssembler{TypeName: "bytes"}.BeginList(0)
}

func (keyAssembler) AssignNull() error {
	return mixins.BytesAssembler{TypeName: "bytes"}.AssignNull()
}

func (keyAssembler) AssignBool(bool) error {
	return mixins.BytesAssembler{TypeName: "bytes"}.AssignBool(false)
}

func (keyAssembler) AssignInt(int64) error {
	return mixins.BytesAssembler{TypeName: "bytes"}.AssignInt(0)
}

func (keyAssembler) AssignFloat(float64) error {
	return mixins.BytesAssembler{TypeName: "bytes"}.AssignFloat(0)
}

func (a keyAssembler) AssignString(s string) error {
	return a.AssignBytes([]byte(s))
}

func (a keyAssembler) AssignBytes(b []byte) error {
	a.parent.assemblingKey = b
	return nil
}

func (keyAssembler) AssignLink(ipld.Link) error {
	return mixins.BytesAssembler{TypeName: "bytes"}.AssignLink(nil)
}

func (a keyAssembler) AssignNode(v ipld.Node) error {
	vs, err := v.AsString()
	if err != nil {
		return err
	}
	return a.AssignString(vs)
}

func (keyAssembler) Prototype() ipld.NodePrototype {
	return basicnode.Prototype__Bytes{}
}

type valueAssembler struct {
	parent *assembler
}

func (valueAssembler) BeginMap(sizeHint int64) (ipld.MapAssembler, error) {
	return mixins.BytesAssembler{TypeName: "bytes"}.BeginMap(0)
}

func (valueAssembler) BeginList(sizeHint int64) (ipld.ListAssembler, error) {
	return mixins.BytesAssembler{TypeName: "bytes"}.BeginList(0)
}

func (a valueAssembler) AssignNull() error {
	return fmt.Errorf("TODO")
	// return a.AssignNode(ipld.Null)
}

func (a valueAssembler) AssignBool(b bool) error {
	builder := basicnode.Prototype.Any.NewBuilder()
	if err := builder.AssignBool(b); err != nil {
		return err
	}
	return a.AssignNode(builder.Build())
}

func (a valueAssembler) AssignInt(i int64) error {
	builder := basicnode.Prototype.Any.NewBuilder()
	if err := builder.AssignInt(i); err != nil {
		return err
	}
	return a.AssignNode(builder.Build())
}

func (a valueAssembler) AssignFloat(f float64) error {
	builder := basicnode.Prototype.Any.NewBuilder()
	if err := builder.AssignFloat(f); err != nil {
		return err
	}
	return a.AssignNode(builder.Build())
}

func (a valueAssembler) AssignString(s string) error {
	builder := basicnode.Prototype.Any.NewBuilder()
	if err := builder.AssignString(s); err != nil {
		return err
	}
	return a.AssignNode(builder.Build())
}

func (a valueAssembler) AssignBytes(b []byte) error {
	builder := basicnode.Prototype.Any.NewBuilder()
	if err := builder.AssignBytes(b); err != nil {
		return err
	}
	return a.AssignNode(builder.Build())
}

func (a valueAssembler) AssignLink(l ipld.Link) error {
	builder := basicnode.Prototype.Any.NewBuilder()
	if err := builder.AssignLink(l); err != nil {
		return err
	}
	return a.AssignNode(builder.Build())
}

func (a valueAssembler) AssignNode(val ipld.Node) error {

	key := a.parent.assemblingKey
	if a.parent.assemblingKey == nil {
		return fmt.Errorf("invalid key")
	}
	a.parent.assemblingKey = nil
	node := a.parent.node
	hash := node.hashKey(key)
	entry := BucketEntry{Key: key, Value: val}
	return node.insertEntry(&node.Hamt, node.bitWidth(), 0, hash, entry)
}

func (valueAssembler) Prototype() ipld.NodePrototype {
	return basicnode.Prototype__Bytes{}
}
