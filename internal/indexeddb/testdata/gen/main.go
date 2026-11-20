// Command gen writes the synthetic IndexedDB fixture in testdata/fixture.
// Run from the repository root: go run ./internal/indexeddb/testdata/gen
// Then make the golden file with golden.py in this folder.
package main

import (
	"log"
	"os"
	"path/filepath"

	"github.com/stn1slv/teamscache/internal/idbtest"
)

func main() {
	root := filepath.Join("internal", "indexeddb", "testdata", "fixture")
	if err := os.RemoveAll(root); err != nil {
		log.Fatal(err)
	}
	obj := func(props ...any) idbtest.Obj {
		var o idbtest.Obj
		for i := 0; i < len(props); i += 2 {
			o = append(o, idbtest.Prop{Key: props[i], Value: props[i+1]})
		}
		return o
	}
	dbs := []idbtest.Database{
		{Number: 1, Name: "alpha", Stores: []idbtest.Store{
			{Name: "plain", Records: []idbtest.Record{
				{Key: "a", Value: obj("text", "hello", "n", 1, "big", 1768557031836.0)},
				{Key: "b", Value: []any{"x", nil, idbtest.Undefined{}, true}},
				{Key: "a", Value: obj("text", "hello again")},
				{Key: "gone", Value: "soon deleted", Deleted: true},
				{Key: "bad", Value: "x", Wrapping: idbtest.Corrupt},
			}},
			{Name: "wrapped", Records: []idbtest.Record{
				{Key: "s", Value: obj("compressed", "ünïcödé ✓ 😀"), Wrapping: idbtest.Snappy},
				{Key: "f", Value: obj("in", "a blob file"), Wrapping: idbtest.Blob},
			}},
		}},
		{Number: 2, Name: "beta", Stores: []idbtest.Store{
			{Name: "plain", Records: []idbtest.Record{{Key: "only", Value: 7}}},
		}},
	}
	if err := idbtest.Write(filepath.Join(root, "leveldb"), filepath.Join(root, "blob"), dbs); err != nil {
		log.Fatal(err)
	}
}
