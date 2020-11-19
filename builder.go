package hamt

import (
	"fmt"
	"os"

	"github.com/ipld/go-ipld-prime"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/ipld/go-ipld-prime/node/mixins"
)

var (
	_ = fmt.Sprint
	_ = os.Stdout
)

var _ ipld.NodePrototype = (*Prototype)(nil)

type Prototype struct {
	BitWidth   int
	BucketSize int

	// hashAlg requires an extra bool, because the zero value can't be used
	// as the default behavior, since the code 0x00 is a valid multicodec
	// code.
	hashAlg    int
	hashAlgSet bool
}

func (p Prototype) WithHashAlg(code int) Prototype {
	p.hashAlg = code
	p.hashAlgSet = true
	return p
}

// These are some multicodec constants we need to support initially.
// TODO: replace them with go-multicodec once the new version is ready.
const (
	Identity    = 0x00
	Sha2_256    = 0x12
	Murmur3_128 = 0x22
)

func (p Prototype) NewBuilder() ipld.NodeBuilder {
	return NewBuilder(p)
}

var _ ipld.NodeBuilder = (*Builder)(nil)

type Builder struct {
	bitWidth   int
	hashAlg    int
	bucketSize int

	node Node
}

func NewBuilder(proto Prototype) *Builder {
	// Set the defaults.
	if proto.BitWidth < 1 {
		proto.BitWidth = 8
	}
	if proto.BucketSize < 1 {
		proto.BucketSize = 3
	}
	if !proto.hashAlgSet {
		proto.hashAlg = Murmur3_128
	}

	return &Builder{
		bitWidth:   proto.BitWidth,
		hashAlg:    proto.hashAlg,
		bucketSize: proto.BucketSize,
	}
}

func (b Builder) WithLinking(builder ipld.LinkBuilder, loader ipld.Loader, storer ipld.Storer) *Builder {
	b.node = *b.node.WithLinking(builder, loader, storer)
	return &b
}

func Build(builder *Builder) *Node {
	return &builder.node
}

func (b *Builder) Build() ipld.Node { return Build(b) }
func (b *Builder) Reset()           { b.node = Node{} }

func (b *Builder) BeginMap(sizeHint int) (ipld.MapAssembler, error) {
	if b.bitWidth < 3 {
		return nil, fmt.Errorf("bitWidth must bee at least 3")
	}
	switch b.hashAlg {
	case Identity, Sha2_256, Murmur3_128:
	default:
		return nil, fmt.Errorf("unsupported hash algorithm: %x", b.hashAlg)
	}
	b.node._HashMapRoot = _HashMapRoot{
		hashAlg:    _Int{b.hashAlg},
		bucketSize: _Int{b.bucketSize},
		hamt: _HashMapNode{
			_map: _Bytes{make([]byte, 1<<(b.bitWidth-3))},
		},
	}
	return &assembler{node: &b.node}, nil
}
func (b *Builder) BeginList(sizeHint int) (ipld.ListAssembler, error) { panic("todo: error?") }
func (b *Builder) AssignNull() error                                  { panic("todo: error?") }
func (b *Builder) AssignBool(bool) error                              { panic("todo: error?") }
func (b *Builder) AssignInt(int) error                                { panic("todo: error?") }
func (b *Builder) AssignFloat(float64) error                          { panic("todo: error?") }
func (b *Builder) AssignString(string) error                          { panic("todo: error?") }
func (b *Builder) AssignBytes([]byte) error                           { panic("todo: error?") }
func (b *Builder) AssignLink(ipld.Link) error                         { panic("todo: error?") }
func (b *Builder) AssignNode(ipld.Node) error                         { panic("todo: error?") }
func (b *Builder) Prototype() ipld.NodePrototype                      { panic("todo: error?") }

type assembler struct {
	node *Node

	assemblingKey []byte
}

type assembleState uint8

const (
	stateInitial assembleState = iota
	stateMidKey
	stateExpectValue
	stateMidValue
	stateFinished
)

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
	return _Bytes__Prototype{}
}

func (a *assembler) ValuePrototype(k string) ipld.NodePrototype {
	return _Any__Prototype{}
}

type keyAssembler struct {
	parent *assembler
}

func (keyAssembler) BeginMap(sizeHint int) (ipld.MapAssembler, error) {
	return mixins.BytesAssembler{"bytes"}.BeginMap(0)
}

func (keyAssembler) BeginList(sizeHint int) (ipld.ListAssembler, error) {
	return mixins.BytesAssembler{"bytes"}.BeginList(0)
}

func (keyAssembler) AssignNull() error {
	return mixins.BytesAssembler{"bytes"}.AssignNull()
}

func (keyAssembler) AssignBool(bool) error {
	return mixins.BytesAssembler{"bytes"}.AssignBool(false)
}

func (keyAssembler) AssignInt(int) error {
	return mixins.BytesAssembler{"bytes"}.AssignInt(0)
}

func (keyAssembler) AssignFloat(float64) error {
	return mixins.BytesAssembler{"bytes"}.AssignFloat(0)
}

func (a keyAssembler) AssignString(s string) error {
	return a.AssignBytes([]byte(s))
}

func (a keyAssembler) AssignBytes(b []byte) error {
	a.parent.assemblingKey = b
	return nil
}

func (keyAssembler) AssignLink(ipld.Link) error {
	return mixins.BytesAssembler{"bytes"}.AssignLink(nil)
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

func (valueAssembler) BeginMap(sizeHint int) (ipld.MapAssembler, error) {
	return mixins.BytesAssembler{"bytes"}.BeginMap(0)
}

func (valueAssembler) BeginList(sizeHint int) (ipld.ListAssembler, error) {
	return mixins.BytesAssembler{"bytes"}.BeginList(0)
}

func (a valueAssembler) AssignNull() error {
	return fmt.Errorf("TODO")
	// return a.AssignNode(ipld.Null)
}

func (a valueAssembler) AssignBool(b bool) error {
	builder := _Any__ReprPrototype{}.NewBuilder()
	if err := builder.AssignBool(b); err != nil {
		return err
	}
	return a.AssignNode(builder.Build())
}

func (a valueAssembler) AssignInt(i int) error {
	builder := _Any__ReprPrototype{}.NewBuilder()
	if err := builder.AssignInt(i); err != nil {
		return err
	}
	return a.AssignNode(builder.Build())
}

func (a valueAssembler) AssignFloat(f float64) error {
	builder := _Any__ReprPrototype{}.NewBuilder()
	if err := builder.AssignFloat(f); err != nil {
		return err
	}
	return a.AssignNode(builder.Build())
}

func (a valueAssembler) AssignString(s string) error {
	builder := _Any__ReprPrototype{}.NewBuilder()
	if err := builder.AssignString(s); err != nil {
		return err
	}
	return a.AssignNode(builder.Build())
}

func (a valueAssembler) AssignBytes(b []byte) error {
	builder := _Any__ReprPrototype{}.NewBuilder()
	if err := builder.AssignBytes(b); err != nil {
		return err
	}
	return a.AssignNode(builder.Build())
}

func (valueAssembler) AssignLink(ipld.Link) error {
	return mixins.BytesAssembler{"bytes"}.AssignLink(nil)
}

func (a valueAssembler) AssignNode(v ipld.Node) error {
	val := v.(*_Any)

	key := a.parent.assemblingKey
	if a.parent.assemblingKey == nil {
		return fmt.Errorf("invalid key")
	}
	a.parent.assemblingKey = nil
	node := a.parent.node
	hash := node.hashKey(key)

	return node.insertEntry(&node.hamt, node.bitWidth(), 0, hash, _BucketEntry{
		_Bytes{key}, *val,
	})
}

func (valueAssembler) Prototype() ipld.NodePrototype {
	return basicnode.Prototype__Bytes{}
}
