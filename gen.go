// +build ignore

package main

import (
	"fmt"
	"os"

	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/schema"
	gengo "github.com/ipld/go-ipld-prime/schema/gen/go"
)

func main() {
	ts := schema.TypeSystem{}
	ts.Init()
	adjCfg := &gengo.AdjunctCfg{
		CfgUnionMemlayout: map[schema.TypeName]string{
			"Any":     "interface",
			"Element": "interface",
		},
		FieldSymbolLowerOverrides: map[gengo.FieldTuple]string{
			{TypeName: "HashMapNode", FieldName: "map"}: "_map",
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
		schema.SpawnUnionRepresentationKinded(map[ipld.ReprKind]schema.TypeName{
			ipld.ReprKind_Bool:   "Bool",
			ipld.ReprKind_Int:    "Int",
			ipld.ReprKind_Float:  "Float",
			ipld.ReprKind_String: "String",
			ipld.ReprKind_Bytes:  "Bytes",
			ipld.ReprKind_Map:    "Map",
			ipld.ReprKind_List:   "List",
			ipld.ReprKind_Link:   "Link",
		}),
	))

	// The schema below follows https://github.com/ipld/specs/blob/master/data-structures/hashmap.md.
	ts.Accumulate(schema.SpawnStruct("HashMapRoot",
		[]schema.StructField{
			schema.SpawnStructField("hashAlg", "Int", false, false),
			schema.SpawnStructField("bucketSize", "Int", false, false),
			schema.SpawnStructField("hamt", "HashMapNode", false, false),
		},
		schema.StructRepresentation_Map{},
	))
	ts.Accumulate(schema.SpawnStruct("HashMapNode",
		[]schema.StructField{
			schema.SpawnStructField("map", "Bytes", false, false),
			schema.SpawnStructField("data", "List__Element", false, false),
		},
		schema.StructRepresentation_Tuple{},
	))
	ts.Accumulate(schema.SpawnList("List__Element",
		"Element", false,
	))
	ts.Accumulate(schema.SpawnUnion("Element",
		[]schema.TypeName{
			"Link__HashMapNode",
			"Bucket",
		},
		schema.SpawnUnionRepresentationKinded(map[ipld.ReprKind]schema.TypeName{
			ipld.ReprKind_Link: "Link__HashMapNode",
			ipld.ReprKind_List: "Bucket",
		}),
	))
	ts.Accumulate(schema.SpawnLinkReference("Link__HashMapNode",
		"HashMapNode",
	))
	ts.Accumulate(schema.SpawnList("Bucket",
		"BucketEntry", false,
	))
	ts.Accumulate(schema.SpawnStruct("BucketEntry",
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
		os.Exit(1)
	}

	gengo.Generate(".", "hamt", ts, adjCfg)
}
