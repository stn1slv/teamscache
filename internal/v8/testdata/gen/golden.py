"""Decodes internal/v8/testdata/corpus/*.bin with ccl_chromium_reader and writes
the normalized result next to each file as *.json (the golden files).

Run from the repository root:
    uv run --no-project --with "git+https://github.com/cclgroupltd/ccl_chromium_reader" \
        python -I internal/v8/testdata/gen/golden.py

The normalized form must match normalize() in internal/v8/corpus_test.go.
"""
import datetime
import io
import json
import pathlib
import re
import struct
import sys

from ccl_chromium_reader.serialization_formats import ccl_v8_value_deserializer as v8

EPOCH = datetime.datetime(1970, 1, 1)


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
    if isinstance(value, bytes):
        return {"$bytes": value.hex()}
    if isinstance(value, datetime.datetime):
        return {"$date_us": str((value - EPOCH) // datetime.timedelta(microseconds=1))}
    if isinstance(value, re.Pattern):
        return {"$regexp": value.pattern}
    if isinstance(value, dict):
        return {"$dict": [[normalize(k), normalize(v)] for k, v in value.items()]}
    if isinstance(value, (list, tuple)):
        return {"$list": [normalize(v) for v in value]}
    if isinstance(value, (set, frozenset)):
        items = [normalize(v) for v in value]
        return {"$set": sorted(items, key=lambda x: json.dumps(x, sort_keys=True))}
    raise TypeError(f"cannot normalize {type(value)}")


def main():
    corpus = pathlib.Path(__file__).resolve().parent.parent / "corpus"
    for path in sorted(corpus.glob("*.bin")):
        sys.setrecursionlimit(10000)
        try:
            value = v8.Deserializer(io.BytesIO(path.read_bytes()), host_object_delegate=None).read()
        except (ValueError, TypeError, AssertionError):
            # The Go decoder must reject this value too.
            normalized = {"$error": True}
        else:
            normalized = normalize(value)
        golden = json.dumps(normalized, ensure_ascii=False, sort_keys=True, indent=1)
        path.with_suffix(".json").write_text(golden + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
