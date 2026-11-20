"""Reads internal/indexeddb/testdata/fixture with ccl_chromium_reader and writes
the records it returns to testdata/golden.json.

Run from the repository root:
    uv run --no-project --with "git+https://github.com/cclgroupltd/ccl_chromium_reader" \
        python -I internal/indexeddb/testdata/gen/golden.py

The record shape must match TestFixtureMatchesCCL in internal/indexeddb.
"""
import json
import pathlib
import struct

from ccl_chromium_reader import ccl_chromium_indexeddb as idb
from ccl_chromium_reader.serialization_formats import ccl_v8_value_deserializer as v8


def normalize(value):
    if value is None:
        return None
    if isinstance(value, v8._Undefined):
        return {"$undefined": True}
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return {"$int": str(value)}
    if isinstance(value, float):
        return {"$float": str(struct.unpack("<Q", struct.pack("<d", value))[0])}
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        return {"$dict": [[normalize(k), normalize(v)] for k, v in value.items()]}
    if isinstance(value, (list, tuple)):
        return {"$list": [normalize(v) for v in value]}
    raise TypeError(f"cannot normalize {type(value)}")


def main():
    testdata = pathlib.Path(__file__).resolve().parent.parent
    fixture = testdata / "fixture"
    db = idb.WrappedIndexDB(fixture / "leveldb", fixture / "blob")
    out = {"databases": [], "records": [], "bad": 0}

    def on_bad(_key, _raw):
        out["bad"] += 1

    for meta in db.database_ids:
        out["databases"].append({"number": meta.dbid_no, "origin": meta.origin, "name": meta.name})
        for store in db[meta.dbid_no]:
            for rec in store.iterate_records(bad_deserializer_data_handler=on_bad):
                out["records"].append({
                    "db": meta.dbid_no, "store": store.name, "key": rec.key.raw_key.hex(),
                    "live": rec.is_live, "value": normalize(rec.value),
                })
    (testdata / "golden.json").write_text(json.dumps(out, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
