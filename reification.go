package hamt

import (
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
)

// Reify looks at an ipld Node and tries to interpret it as a HAMT;
// if successful, it returns the synthetic Node which can be used to access the entire HAMT as if it was a single simple map.
//
// Presumably the node given as a parameter is "raw" data model.  (Nothing enforces this, however.)
//
// Reify is one of the ways you can create a HAMT node, but it's not the only one.
// Reify is most suitable if you've got a bunch of data you already parsed into a tree raw.
// Other approaches include using the synthetic builder (if you just want to engage at the "build a map" level),
// or using Schemas to denote where the HAMT should appear (in which case
// loading data while using the schema should automatically reify the HAMT without further action required).
func Reify(lnkCtx ipld.LinkContext, maybeHamt ipld.Node, lsys *ipld.LinkSystem) (ipld.Node, error) {
	n := Node{
		modeFilecoin:  false,
		linkSystem:    *lsys,
		linkPrototype: cidlink.LinkPrototype{},
	}
	if lnkCtx.LinkNode != nil {
		l, err := lnkCtx.LinkNode.AsLink()
		if err != nil {
			return nil, err
		}
		n.linkPrototype = l.Prototype()
	}

	// see if node looks like a hamt root:
	if bs, err := maybeHamt.LookupByString("bucketSize"); err == nil && bs.Kind() == ipld.Kind_Int {
		hmrb := Type.HashMapRoot__Repr.NewBuilder()
		if err := hmrb.AssignNode(maybeHamt); err != nil {
			return nil, err
		}
		hmr := hmrb.Build().(*_HashMapRoot)
		n._HashMapRoot = *hmr
	} else {
		n.modeFilecoin = true
		hmnb := Type.HashMapNode__Repr.NewBuilder()
		if err := hmnb.AssignNode(maybeHamt); err != nil {
			return nil, err
		}
		hmn := hmnb.Build().(*_HashMapNode)
		n._HashMapRoot.hamt = *hmn
	}

	return &n, nil
}

// Substrate returns the representation of the ADL
func (n *Node) Substrate() ipld.Node {
	if n.modeFilecoin {
		// A Filecoin v3 HAMT is encoded as just the root node, without
		// the config parameters.
		return &n.hamt
	}
	// An IPLD spec HAMT is encoded including an extra root node which
	// includes explicit config parameters.
	return &n._HashMapRoot
}
