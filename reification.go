package hamt

import (
	"github.com/ipld/go-ipld-prime"
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
func Reify(root ipld.Node) (ipld.Node, error) {
	panic("nyi")
}
