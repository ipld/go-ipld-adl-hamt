package hamt

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"hash"
	"math"
	"math/bits"

	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/node/mixins"
	"github.com/multiformats/go-multicodec"
	"github.com/twmb/murmur3"
)

var _ ipld.Node = (*Node)(nil)

type Node struct {
	modeFilecoin bool
	_HashMapRoot

	linkSystem    ipld.LinkSystem
	linkPrototype ipld.LinkPrototype
}

func (n Node) WithLinking(system ipld.LinkSystem, proto ipld.LinkPrototype) *Node {
	n.linkSystem = system
	n.linkPrototype = proto
	return &n
}

func (n *Node) bitWidth() int {
	if n.modeFilecoin {
		return 5
	}
	// bitWidth is inferred from the map length via the equation:
	//
	//     log2(byteLength(map) x 8)
	//
	// Since byteLength(map) is a power of 2, we don't need the expensive
	// float-based math.Log2; we can simply count the trailing zero bits.

	// Prevent len==0 from giving us a byteLength of 32 or 64,
	// since a zero integer is all trailing zeros.
	// We also prevent overflows when converting to uint32.
	// Such invalid states get coalescled to -1, like _bucketSize.
	mapLength := len(n.hamt._map.x)
	if mapLength <= 0 || int64(mapLength) > math.MaxUint32 {
		return -1
	}

	return bits.TrailingZeros32(uint32(mapLength)) + 3
}

func (n *Node) _bucketSize() int {
	if n.modeFilecoin {
		return 3
	}
	// Any negative bucket size,
	// or a bucket size large enough to cause enormous allocations,
	// gets coalesced to -1.
	// We also prevent overflows and underflows, as we convert int64 to int.
	const maxBucketSize = 1 << 20 // 1MiB
	bucketSize := n.bucketSize.x
	if bucketSize < 0 || bucketSize > maxBucketSize {
		return -1
	}
	return int(bucketSize)
}

func (*Node) Kind() ipld.Kind {
	return ipld.Kind_Map
}

func (n *Node) LookupByString(s string) (ipld.Node, error) {
	key := []byte(s)
	hash := n.hashKey(key)
	return n.lookupValue(&n.hamt, n.bitWidth(), 0, hash, key)
}

func (*Node) LookupByNode(ipld.Node) (ipld.Node, error) {
	panic("TODO")
}

func (*Node) LookupBySegment(seg ipld.PathSegment) (ipld.Node, error) {
	panic("TODO")
}

func (n *Node) MapIterator() ipld.MapIterator {
	return &nodeIterator{
		modeFilecoin:     n.modeFilecoin,
		nodeIteratorStep: nodeIteratorStep{node: &n.hamt},
	}
}

type nodeIteratorStep struct {
	node *_HashMapNode

	mapIndex int

	bucket      *_Bucket
	bucketIndex int
}

type nodeIterator struct {
	modeFilecoin bool

	parents []nodeIteratorStep

	nodeIteratorStep

	next *_BucketEntry
}

func (t *nodeIterator) Done() bool {
	if t.bucket != nil {
		// We are iterating over a bucket.
		if t.bucketIndex < len(t.bucket.x) {
			t.next = &t.bucket.x[t.bucketIndex]
			t.bucketIndex++
			return false
		}
		// We've done all entries in this bucket.
		// Continue to the next element.
		t.bucket = nil
		t.bucketIndex = 0
	}

	// We are not in the middle of a sub-node or bucket.
	// Find the next bit set in the bitmap.
	bitmap := t.node._map.x
	found := false
	for t.mapIndex < len(bitmap)*8 {
		var bit bool
		if t.modeFilecoin {
			bit = bitsetGetv3(bitmap, t.mapIndex)
		} else {
			bit = bitsetGet(bitmap, t.mapIndex)
		}
		if bit {
			found = true
			break
		}
		t.mapIndex++
	}

	if !found {
		// The rest of the elements are all empty.
		// If we're in a sub-node, continue iterating the parent.
		// Otherwise, we're done.
		if len(t.parents) == 0 {
			return true
		}
		parent := t.parents[len(t.parents)-1]
		t.parents = t.parents[:len(t.parents)-1]
		t.nodeIteratorStep = parent
		return t.Done()
	}

	var dataIndex int
	if t.modeFilecoin {
		dataIndex = onesCountRangev3(t.node._map.x, t.mapIndex)
	} else {
		dataIndex = onesCountRange(t.node._map.x, t.mapIndex)
	}
	// We found an element; make sure we don't iterate over it again.
	t.mapIndex++

	switch element := t.node.data.x[dataIndex].x.(type) {
	case *_Bucket:
		t.bucket = element
		t.bucketIndex = 1

		t.next = &element.x[0]
		return false
	default:
		panic(fmt.Sprintf("unexpected element type: %T", element))
	}
}

func (t *nodeIterator) Next() (key, value ipld.Node, _ error) {
	if t.next == nil {
		return nil, nil, ipld.ErrIteratorOverread{}
	}
	// Bits of ipld-prime expect map keys to be strings.
	// By design, our HAMT uses arbitrary bytes as keys.
	// Lucky for us, in Go a string can still represent arbitrary bytes.
	// So for now, return the key as a String node.
	// TODO: revisit this if the state of pathing with mixed kind keys advances.
	key = (&_String{x: string(t.next.key.x)}).Representation()
	value = t.next.value.Representation()
	return key, value, nil
}

func (n *Node) Length() int64 {
	count, err := n.count(&n.hamt, n.bitWidth(), 0)
	if err != nil {
		panic(fmt.Sprintf("TODO: what to do with this error: %v", err))
	}
	return count
}

func (n *Node) count(node *_HashMapNode, bitWidth, depth int) (int64, error) {
	count := int64(0)
	for _, element := range node.data.x {
		switch element := element.x.(type) {
		case *_Bucket:
			count += int64(len(element.x))
		case *_Link__HashMapNode:
			// TODO: cache loading links
			childNode, err := n.linkSystem.Load(
				ipld.LinkContext{Ctx: context.TODO()},
				element.x,
				_HashMapNode__Prototype{},
			)
			if err != nil {
				return 0, err
			}
			child := childNode.(*_HashMapNode)
			childCount, err := n.count(child, bitWidth, depth+1)
			if err != nil {
				return 0, err
			}
			count += childCount
		default:
			panic(fmt.Sprintf("unknown element type: %T", element))
		}
	}
	return count, nil
}

func (*Node) LookupByIndex(idx int64) (ipld.Node, error) {
	return mixins.Map{TypeName: "hamt.Node"}.LookupByIndex(0)
}

func (*Node) ListIterator() ipld.ListIterator {
	return mixins.Map{TypeName: "hamt.Node"}.ListIterator()
}

func (*Node) IsAbsent() bool {
	return false
}

func (*Node) IsNull() bool {
	return false
}

func (*Node) AsBool() (bool, error) {
	return mixins.Map{TypeName: "hamt.Node"}.AsBool()
}

func (*Node) AsInt() (int64, error) {
	return mixins.Map{TypeName: "hamt.Node"}.AsInt()
}

func (*Node) AsFloat() (float64, error) {
	return mixins.Map{TypeName: "hamt.Node"}.AsFloat()
}

func (*Node) AsString() (string, error) {
	return mixins.Map{TypeName: "hamt.Node"}.AsString()
}

func (*Node) AsBytes() ([]byte, error) {
	return mixins.Map{TypeName: "hamt.Node"}.AsBytes()
}

func (*Node) AsLink() (ipld.Link, error) {
	return mixins.Map{TypeName: "hamt.Node"}.AsLink()
}

func (n *Node) hashKey(b []byte) []byte {
	var hasher hash.Hash
	if n.modeFilecoin {
		hasher = sha256.New()
	} else {
		switch c := multicodec.Code(n.hashAlg.x); c {
		case multicodec.Identity:
			return b
		case multicodec.Sha2_256:
			hasher = sha256.New()
		case multicodec.Murmur3X64_64:
			hasher = murmur3.New128()
		default:
			// TODO: could we reach this? the builder already handles this
			// case, but other entry points like Reify don't.
			panic(fmt.Sprintf("unsupported hash algorithm: %s", c))
		}
	}
	hasher.Write(b)
	return hasher.Sum(nil)
}

func (n *Node) insertEntry(node *_HashMapNode, bitWidth, depth int, hash []byte, entry _BucketEntry) error {
	from := depth * bitWidth
	index := rangedInt(hash, from, from+bitWidth)

	var dataIndex int
	if n.modeFilecoin {
		dataIndex = onesCountRangev3(node._map.x, index)
	} else {
		dataIndex = onesCountRange(node._map.x, index)
	}
	var exists bool
	if n.modeFilecoin {
		exists = bitsetGetv3(node._map.x, index)
	} else {
		exists = bitsetGet(node._map.x, index)
	}
	if !exists {
		// Insert a new bucket at dataIndex.
		bucket := &_Bucket{[]_BucketEntry{entry}}
		node.data.x = append(node.data.x[:dataIndex],
			append([]_Element{{bucket}}, node.data.x[dataIndex:]...)...)
		if n.modeFilecoin {
			node._map.x = bitsetSetv3(node._map.x, index)
		} else {
			bitsetSet(node._map.x, index)
		}
		return nil
	}
	// TODO: fix links up the chain too
	switch element := node.data.x[dataIndex].x.(type) {
	case *_Bucket:
		if len(element.x) < n._bucketSize() {
			i, _ := lookupBucketEntry(element.x, entry.key.x)
			if i >= 0 {
				// Replace an existing key.
				element.x[i] = entry
			} else {
				// Add a new key.
				// TODO: keep the list sorted
				element.x = append(element.x, entry)
			}
			node.data.x[dataIndex].x = element
			break
		}
		child := &_HashMapNode{
			_map: _Bytes{make([]byte, 1<<(bitWidth-3))},
		}
		for _, entry := range element.x {
			hash := n.hashKey(entry.key.x)
			n.insertEntry(child, bitWidth, depth+1, hash, entry)
		}
		n.insertEntry(child, bitWidth, depth+1, hash, entry)
		link, err := n.linkSystem.Store(
			ipld.LinkContext{Ctx: context.TODO()},
			n.linkPrototype,
			child,
		)
		if err != nil {
			return err
		}

		node.data.x[dataIndex].x = &_Link__HashMapNode{link}
	case *_Link__HashMapNode:
		// TODO: cache loading links
		childNode, err := n.linkSystem.Load(
			ipld.LinkContext{Ctx: context.TODO()},
			element.x,
			_HashMapNode__Prototype{},
		)
		if err != nil {
			return err
		}
		child := childNode.(*_HashMapNode)

		n.insertEntry(child, bitWidth, depth+1, hash, entry)

		link, err := n.linkSystem.Store(
			ipld.LinkContext{Ctx: context.TODO()},
			n.linkPrototype,
			child,
		)
		if err != nil {
			return err
		}

		node.data.x[dataIndex].x = &_Link__HashMapNode{link}
	default:
		panic(fmt.Sprintf("unexpected element type: %T", element))
	}
	return nil
}

func lookupBucketEntry(entries []_BucketEntry, key []byte) (idx int, value _Any) {
	// TODO: to better support large buckets, should this be a
	// binary search?
	for i, entry := range entries {
		if bytes.Equal(entry.key.x, key) {
			return i, entry.value
		}
	}
	return -1, _Any{}
}

func (n *Node) lookupValue(node *_HashMapNode, bitWidth, depth int, hash, key []byte) (ipld.Node, error) {
	from := depth * bitWidth
	index := rangedInt(hash, from, from+bitWidth)

	var exists bool
	if n.modeFilecoin {
		exists = bitsetGetv3(node._map.x, index)
	} else {
		exists = bitsetGet(node._map.x, index)
	}
	if !exists {
		return nil, nil
	}
	var dataIndex int
	if n.modeFilecoin {
		dataIndex = onesCountRangev3(node._map.x, index)
	} else {
		dataIndex = onesCountRange(node._map.x, index)
	}
	switch element := node.data.x[dataIndex].x.(type) {
	case *_Bucket:
		i, value := lookupBucketEntry(element.x, key)
		if i >= 0 {
			return value.Representation(), nil
		}
	case *_Link__HashMapNode:
		// TODO: cache loading links
		childNode, err := n.linkSystem.Load(
			ipld.LinkContext{Ctx: context.TODO()},
			element.x,
			_HashMapNode__Prototype{},
		)
		if err != nil {
			return nil, err
		}
		child := childNode.(*_HashMapNode)
		return n.lookupValue(child, bitWidth, depth+1, hash, key)
	default:
		panic(fmt.Sprintf("unknown element type: %T", element))
	}
	return nil, nil
}
