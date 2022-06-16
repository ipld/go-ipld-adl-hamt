package hamt

import (
	"errors"
)

// ErrMalformedHamt is returned whenever a block intended as a HAMT node does
// not conform to the expected form that a block may take. This can occur
// during block-load where initial validation takes place or during traversal
// where certain conditions are expected to be met.
var ErrMalformedHamt = errors.New("malformed HAMT node")
