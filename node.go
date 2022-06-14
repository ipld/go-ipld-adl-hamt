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
	"github.com/ipld/go-ipld-prime/datamodel"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/ipld/go-ipld-prime/node/bindnode"
	"github.com/ipld/go-ipld-prime/node/mixins"
	"github.com/multiformats/go-multicodec"
	"github.com/twmb/murmur3"
)

var _ ipld.Node = (*Node)(nil)

type Node struct {
	HashMapRoot
	modeFilecoin  bool
	linkSystem    ipld.LinkSystem
	linkPrototype ipld.LinkPrototype
}

func (n Node) Prototype() datamodel.NodePrototype {
	return HashMapRootPrototype
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
	// Such invalid states get coalesced to -1, like BucketSize.
	mapLength := len(n.Hamt.Map)
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
	bucketSize := n.HashMapRoot.BucketSize
	if bucketSize < 0 || bucketSize > maxBucketSize {
		return -1
	}
	return bucketSize
}

func (*Node) Kind() ipld.Kind {
	return ipld.Kind_Map
}

func (n *Node) LookupByString(s string) (ipld.Node, error) {
	key := []byte(s)
	hk := n.hashKey(key)
	return n.lookupValue(&n.Hamt, n.bitWidth(), 0, hk, key)
}

func (*Node) LookupByNode(ipld.Node) (ipld.Node, error) {
	panic("TODO")
}

func (*Node) LookupBySegment(seg ipld.PathSegment) (ipld.Node, error) {
	panic("TODO")
}

func (n *Node) MapIterator() ipld.MapIterator {
	return &nodeIterator{
		ls:               n.linkSystem,
		modeFilecoin:     n.modeFilecoin,
		nodeIteratorStep: nodeIteratorStep{node: &n.Hamt},
	}
}

type nodeIteratorStep struct {
	node *HashMapNode

	mapIndex int

	bucket      *Bucket
	bucketIndex int
}

type nodeIterator struct {
	modeFilecoin bool

	parents []nodeIteratorStep

	nodeIteratorStep

	next *BucketEntry
	ls   ipld.LinkSystem

	// doneErr stores errors that may occur during Done call, which are later returned by the call
	// to Next. See MapIterator.Done.
	doneErr error
}

func (t *nodeIterator) Done() bool {
	if t.bucket != nil {
		// We are iterating over a bucket.
		if t.bucketIndex < len(*t.bucket) {
			t.next = &((*t.bucket)[t.bucketIndex])
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
	bitmap := t.node.Map
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
		dataIndex = onesCountRangev3(t.node.Map, t.mapIndex)
	} else {
		dataIndex = onesCountRange(t.node.Map, t.mapIndex)
	}
	// We found an element; make sure we don't iterate over it again.
	t.mapIndex++

	element := t.node.Data[dataIndex]
	switch {
	case element.Bucket != nil:
		t.bucket = element.Bucket
		t.bucketIndex = 1
		t.next = &(*element.Bucket)[0]
		return false
	case element.HashMapNode != nil:
		childNode, err := t.ls.Load(
			ipld.LinkContext{Ctx: context.TODO()},
			*element.HashMapNode,
			HashMapNodePrototype,
		)
		if err != nil {
			t.doneErr = fmt.Errorf("failed to load child node from linksystem: %w", err)
			return false
		}
		child, ok := bindnode.Unwrap(childNode).(*HashMapNode)
		if !ok {
			t.doneErr = fmt.Errorf("failed to unwrap child node: %w", err)
			return false
		}
		t.parents = append(t.parents, nodeIteratorStep{node: child})
		return t.Done()
	default:
		t.doneErr = fmt.Errorf("unexpected element type: %v", element)
		return false
	}
}

func (t *nodeIterator) Next() (key, value ipld.Node, _ error) {

	// Check if any errors were encountered during call to Done.
	if t.doneErr != nil {
		return nil, nil, t.doneErr
	}

	if t.next == nil {
		return nil, nil, ipld.ErrIteratorOverread{}
	}
	// Bits of ipld-prime expect map keys to be strings.
	// By design, our HAMT uses arbitrary bytes as keys.
	// Lucky for us, in Go a string can still represent arbitrary bytes.
	// So for now, return the key as a String node.
	// TODO: revisit this if the state of pathing with mixed kind keys advances.
	key = basicnode.NewString(string(t.next.Key))
	return key, t.next.Value, nil
}

func (n *Node) Length() int64 {
	count, err := n.count(&n.Hamt, n.bitWidth(), 0)
	if err != nil {
		panic(fmt.Sprintf("TODO: what to do with this error: %v", err))
	}
	return count
}

func (n *Node) count(node *HashMapNode, bitWidth, depth int) (int64, error) {
	count := int64(0)
	for _, element := range node.Data {
		switch {
		case element.Bucket != nil:
			count += int64(len(*element.Bucket))
		case element.HashMapNode != nil:
			// TODO: cache loading links
			childNode, err := n.linkSystem.Load(
				ipld.LinkContext{Ctx: context.TODO()},
				*element.HashMapNode,
				HashMapNodePrototype,
			)
			if err != nil {
				return 0, err
			}
			child, ok := bindnode.Unwrap(childNode).(*HashMapNode)
			if !ok {
				panic(fmt.Sprintf("unexpected node type: %v", childNode))
			}
			childCount, err := n.count(child, bitWidth, depth+1)
			if err != nil {
				return 0, err
			}
			count += childCount
		default:
			panic(fmt.Sprintf("unknown element type: %v", element))
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
		switch c := n.HashAlg; c {
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

func (n *Node) insertEntry(node *HashMapNode, bitWidth, depth int, hash []byte, entry BucketEntry) error {
	from := depth * bitWidth
	index := rangedInt(hash, from, from+bitWidth)

	var dataIndex int
	if n.modeFilecoin {
		dataIndex = onesCountRangev3(node.Map, index)
	} else {
		dataIndex = onesCountRange(node.Map, index)
	}
	var exists bool
	if n.modeFilecoin {
		exists = bitsetGetv3(node.Map, index)
	} else {
		exists = bitsetGet(node.Map, index)
	}
	if !exists {
		// Insert a new bucket at dataIndex.
		bucket := Bucket([]BucketEntry{entry})
		node.Data = append(node.Data[:dataIndex],
			append([]Element{{Bucket: &bucket}}, node.Data[dataIndex:]...)...)
		if n.modeFilecoin {
			node.Map = bitsetSetv3(node.Map, index)
		} else {
			bitsetSet(node.Map, index)
		}
		return nil
	}
	// TODO: fix links up the chain too
	element := node.Data[dataIndex]
	switch {
	case element.Bucket != nil:
		bucket := *element.Bucket
		if len(bucket) < n._bucketSize() {
			i, _ := lookupBucketEntry(bucket, entry.Key)
			if i >= 0 {
				// Replace an existing key.
				bucket[i] = entry
			} else {
				// Add a new key.
				// TODO: keep the list sorted
				bucket = append(bucket, entry)
			}
			element.Bucket = &bucket
			node.Data[dataIndex] = element
			break
		}
		child := &HashMapNode{
			Map: make([]byte, 1<<(bitWidth-3)),
		}
		for _, entry := range bucket {
			hk := n.hashKey(entry.Key)
			if err := n.insertEntry(child, bitWidth, depth+1, hk, entry); err != nil {
				return err
			}
		}
		if err := n.insertEntry(child, bitWidth, depth+1, hash, entry); err != nil {
			return err
		}

		childNode := bindnode.Wrap(child, HashMapNodePrototype.Type())

		link, err := n.linkSystem.Store(
			ipld.LinkContext{Ctx: context.TODO()},
			n.linkPrototype,
			childNode,
		)
		if err != nil {
			return err
		}
		node.Data[dataIndex] = Element{HashMapNode: &link}
	case element.HashMapNode != nil:
		// TODO: cache loading links
		childNode, err := n.linkSystem.Load(
			ipld.LinkContext{Ctx: context.TODO()},
			*element.HashMapNode,
			HashMapNodePrototype,
		)
		if err != nil {
			return err
		}
		child := bindnode.Unwrap(childNode).(*HashMapNode)

		if err := n.insertEntry(child, bitWidth, depth+1, hash, entry); err != nil {
			return err
		}

		link, err := n.linkSystem.Store(
			ipld.LinkContext{Ctx: context.TODO()},
			n.linkPrototype,
			childNode,
		)
		if err != nil {
			return err
		}

		node.Data[dataIndex] = Element{HashMapNode: &link}
	default:
		panic(fmt.Sprintf("unexpected element type: %T", element))
	}
	return nil
}

func lookupBucketEntry(entries Bucket, key []byte) (idx int, value ipld.Node) {
	// TODO: to better support large buckets, should this be a
	//       binary search?
	for i, entry := range entries {
		if bytes.Equal(entry.Key, key) {
			return i, entry.Value
		}
	}
	return -1, nil
}

func (n *Node) lookupValue(node *HashMapNode, bitWidth, depth int, hash, key []byte) (ipld.Node, error) {
	from := depth * bitWidth
	index := rangedInt(hash, from, from+bitWidth)

	var exists bool
	if n.modeFilecoin {
		exists = bitsetGetv3(node.Map, index)
	} else {
		exists = bitsetGet(node.Map, index)
	}
	if !exists {
		return nil, nil
	}
	var dataIndex int
	if n.modeFilecoin {
		dataIndex = onesCountRangev3(node.Map, index)
	} else {
		dataIndex = onesCountRange(node.Map, index)
	}

	element := node.Data[dataIndex]
	switch {
	case element.Bucket != nil:
		i, value := lookupBucketEntry(*element.Bucket, key)
		if i >= 0 {
			return value, nil
		}
	case element.HashMapNode != nil:
		// TODO: cache loading links
		childNode, err := n.linkSystem.Load(
			ipld.LinkContext{Ctx: context.TODO()},
			*element.HashMapNode,
			HashMapNodePrototype,
		)
		if err != nil {
			return nil, err
		}
		child, ok := bindnode.Unwrap(childNode).(*HashMapNode)
		if !ok {
			return nil, fmt.Errorf("unexpected node type: %v", childNode)
		}
		return n.lookupValue(child, bitWidth, depth+1, hash, key)
	default:
		panic(fmt.Sprintf("unknown element type: %T", element))
	}
	return nil, nil
}
