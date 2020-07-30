package hamt

// Type is a struct embeding a NodePrototype/Type for every Node implementation in this package.
// One of its major uses is to start the construction of a value.
// You can use it like this:
//
// 		hamt.Type.YourTypeName.NewBuilder().BeginMap() //...
//
// and:
//
// 		hamt.Type.OtherTypeName.NewBuilder().AssignString("x") // ...
//
var Type typeSlab

type typeSlab struct {
	Any                      _Any__Prototype
	Any__Repr                _Any__ReprPrototype
	Bool                     _Bool__Prototype
	Bool__Repr               _Bool__ReprPrototype
	Bucket                   _Bucket__Prototype
	Bucket__Repr             _Bucket__ReprPrototype
	Bytes                    _Bytes__Prototype
	Bytes__Repr              _Bytes__ReprPrototype
	Float                    _Float__Prototype
	Float__Repr              _Float__ReprPrototype
	Int                      _Int__Prototype
	Int__Repr                _Int__ReprPrototype
	InteriorNode             _InteriorNode__Prototype
	InteriorNode__Repr       _InteriorNode__ReprPrototype
	KV                       _KV__Prototype
	KV__Repr                 _KV__ReprPrototype
	Link                     _Link__Prototype
	Link__Repr               _Link__ReprPrototype
	Link__InteriorNode       _Link__InteriorNode__Prototype
	Link__InteriorNode__Repr _Link__InteriorNode__ReprPrototype
	List                     _List__Prototype
	List__Repr               _List__ReprPrototype
	List__Pointer            _List__Pointer__Prototype
	List__Pointer__Repr      _List__Pointer__ReprPrototype
	Map                      _Map__Prototype
	Map__Repr                _Map__ReprPrototype
	Pointer                  _Pointer__Prototype
	Pointer__Repr            _Pointer__ReprPrototype
	String                   _String__Prototype
	String__Repr             _String__ReprPrototype
}
