package hamtregen

import (
	"fmt"

	"github.com/ipld/go-ipld-prime/schema"
	gengo "github.com/ipld/go-ipld-prime/schema/gen/go"
)

func init() {
	ts := schema.TypeSystem{}
	ts.Init()
	adjCfg := &gengo.AdjunctCfg{
		CfgUnionMemlayout: map[schema.TypeName]string{
			"Any": "interface",
		},
	}

	// Prelude.  (This is boilerplate; it will be injected automatically by the schema libraries in the future, but isn't yet.)
	ts.Accumulate(schema.SpawnBool("Bool"))
	ts.Accumulate(schema.SpawnInt("Int"))
	ts.Accumulate(schema.SpawnFloat("Float"))
	ts.Accumulate(schema.SpawnString("String"))
	ts.Accumulate(schema.SpawnBytes("Bytes"))
	ts.Accumulate(schema.SpawnMap("Map",
		"String", "Any", true,
	))
	ts.Accumulate(schema.SpawnList("List",
		"Any", true,
	))
	ts.Accumulate(schema.SpawnLink("Link"))
	ts.Accumulate(schema.SpawnUnion("Any",
		[]schema.TypeName{
			"Bool",
			"Int",
			"Float",
			"String",
			"Bytes",
			"Map",
			"List",
			"Link",
		},
		schema.SpawnUnionRepresentationKeyed(map[string]schema.TypeName{ // FIXME: this should be kinded
			"bool":   "Bool",
			"int":    "Int",
			"float":  "Float",
			"string": "String",
			"bytes":  "Bytes",
			"map":    "Map",
			"list":   "List",
			"link":   "Link",
		}),
	))

	// Interior structure of the HAMT ADL.
	ts.Accumulate(schema.SpawnStruct("InteriorNode",
		[]schema.StructField{
			schema.SpawnStructField("bitfield", "Bytes", false, false),
			schema.SpawnStructField("pointers", "List__Pointer", false, false),
		},
		schema.StructRepresentation_Tuple{},
	))
	ts.Accumulate(schema.SpawnList("List__Pointer", // would be implicitly created by fields refering to a `[Pointer]` type, but our schema package is not this clever yet, so I've done it by hand.
		"Pointer", false,
	))
	ts.Accumulate(schema.SpawnUnion("Pointer",
		[]schema.TypeName{
			"Bucket",
			"Link__InteriorNode",
		},
		schema.SpawnUnionRepresentationKeyed(map[string]schema.TypeName{ // FUTURE: will probably become kinded, but currently is keyed in this way in upstream spec.
			"0": "Bucket",
			"1": "Link__InteriorNode",
		}),
	))
	ts.Accumulate(schema.SpawnLinkReference("Link__InteriorNode", // would be implicitly created by fields refering to a `[InteriorNode]` type, but our schema package is not this clever yet, so I've done it by hand.
		"InteriorNode",
	))
	ts.Accumulate(schema.SpawnList("Bucket",
		"KV", false,
	))
	ts.Accumulate(schema.SpawnStruct("KV",
		[]schema.StructField{
			schema.SpawnStructField("key", "Bytes", false, false),
			schema.SpawnStructField("value", "Any", false, false),
		},
		schema.StructRepresentation_Tuple{},
	))

	if errs := ts.ValidateGraph(); errs != nil {
		for _, err := range errs {
			fmt.Printf("- %s\n", err)
		}
		panic("not happening")
	}

	gengo.Generate("..", "hamt", ts, adjCfg)
}
