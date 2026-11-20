// Package idbtest writes synthetic Chromium IndexedDB LevelDB folders for
// tests. It encodes keys and values the way Chromium does, but only the parts
// that the readers in this module use.
package idbtest

import (
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"unicode/utf16"

	"github.com/golang/snappy"
	goleveldb "github.com/syndtr/goleveldb/leveldb"
)

// Obj is a JavaScript object with ordered properties.
type Obj []Prop

// Prop is one property of an Obj.
type Prop struct {
	Key   any
	Value any
}

// Undefined is the JavaScript undefined value.
type Undefined struct{}

// Wrapping says how a record value is stored.
type Wrapping int

// Wrappings.
const (
	Plain   Wrapping = iota // a normal Blink envelope with a trailer
	Snappy                  // compressed with Snappy
	Blob                    // stored in a blob file
	Corrupt                 // a value without the Blink version tag
)

// Record is one object store record.
type Record struct {
	Key      string
	Value    any
	Wrapping Wrapping
	// Deleted writes a delete for Key after the value.
	Deleted bool
}

// Store is an object store with records.
type Store struct {
	Name    string
	Records []Record
}

// Database is an IndexedDB database.
type Database struct {
	Number uint64
	Name   string
	Stores []Store
}

// Origin is the origin all test databases belong to.
const Origin = "https_example.test_0@1"

// Write writes the databases to a LevelDB in dir and blob files to blobDir.
func Write(dir, blobDir string, dbs []Database) error {
	db, err := goleveldb.OpenFile(dir, nil)
	if err != nil {
		return fmt.Errorf("open leveldb: %w", err)
	}
	blobNumber := uint64(1)
	for _, d := range dbs {
		key := append([]byte{0, 0, 0, 0, 0xc9}, idbString(Origin)...)
		key = append(key, idbString(d.Name)...)
		if err := db.Put(key, truncatedInt(d.Number), nil); err != nil {
			return err
		}
		meta := makePrefix(d.Number, 0, 0)
		if err := db.Put(append(clone(meta), 3), truncatedInt(uint64(len(d.Stores))), nil); err != nil {
			return err
		}
		for i, s := range d.Stores {
			storeID := uint64(i + 1)
			nameKey := append(clone(meta), 50)
			nameKey = binary.AppendUvarint(nameKey, storeID)
			nameKey = append(nameKey, 0)
			if err := db.Put(nameKey, utf16BE(s.Name), nil); err != nil {
				return err
			}
			for _, r := range s.Records {
				idbKey := append([]byte{1}, idbString(r.Key)...)
				recordKey := append(makePrefix(d.Number, storeID, 1), idbKey...)
				envelope := Envelope(r.Value)
				switch r.Wrapping {
				case Plain:
				case Corrupt:
					envelope = envelope[1:]
				case Snappy:
					envelope = append([]byte{0xff, 0x11, 0x02}, snappy.Encode(nil, envelope)...)
				case Blob:
					path := filepath.Join(blobDir, fmt.Sprintf("%x", d.Number), fmt.Sprintf("%02x", blobNumber>>8), fmt.Sprintf("%x", blobNumber))
					if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
						return err
					}
					if err := os.WriteFile(path, envelope, 0o600); err != nil {
						return err
					}
					entry := []byte{0}
					entry = binary.AppendUvarint(entry, blobNumber)
					entry = binary.AppendUvarint(entry, 0) // mime type length
					entry = binary.AppendUvarint(entry, uint64(len(envelope)))
					entryKey := append(makePrefix(d.Number, storeID, 3), idbKey...)
					if err := db.Put(entryKey, entry, nil); err != nil {
						return err
					}
					envelope = binary.AppendUvarint([]byte{0xff, 0x11, 0x01}, uint64(len(envelope)))
					envelope = binary.AppendUvarint(envelope, 0) // index in the blob entry
					blobNumber++
				}
				value := append([]byte{1}, envelope...) // varint record version, then the envelope
				if err := db.Put(recordKey, value, nil); err != nil {
					return err
				}
				if r.Deleted {
					if err := db.Delete(recordKey, nil); err != nil {
						return err
					}
				}
			}
		}
	}
	return db.Close()
}

// Envelope returns a Blink envelope (wire format version 21, with a trailer)
// holding the V8 serialization of v.
func Envelope(v any) []byte {
	out := []byte{0xff, 21, 0xfe}
	out = append(out, make([]byte, 12)...) // trailer offset and size, unused
	return append(out, Serialize(v)...)
}

// Serialize writes v in the V8 serialization format, version 15. It supports
// nil, Undefined, bool, int, float64, string, Obj and []any.
func Serialize(v any) []byte {
	return writeValue([]byte{0xff, 15}, v)
}

func writeValue(out []byte, v any) []byte {
	switch x := v.(type) {
	case nil:
		return append(out, '0')
	case Undefined:
		return append(out, '_')
	case bool:
		if x {
			return append(out, 'T')
		}
		return append(out, 'F')
	case int:
		out = append(out, 'I')
		return binary.AppendUvarint(out, uint64((int64(x)<<1)^(int64(x)>>63)))
	case float64:
		out = append(out, 'N')
		return binary.LittleEndian.AppendUint64(out, math.Float64bits(x))
	case string:
		return writeString(out, x)
	case Obj:
		out = append(out, 'o')
		for _, p := range x {
			out = writeValue(out, p.Key)
			out = writeValue(out, p.Value)
		}
		out = append(out, '{')
		return binary.AppendUvarint(out, uint64(len(x)))
	case []any:
		out = append(out, 'A')
		out = binary.AppendUvarint(out, uint64(len(x)))
		for _, item := range x {
			out = writeValue(out, item)
		}
		out = append(out, '$', 0)
		return binary.AppendUvarint(out, uint64(len(x)))
	default:
		panic(fmt.Sprintf("idbtest: cannot serialize %T", v))
	}
}

func writeString(out []byte, s string) []byte {
	runes := []rune(s)
	oneByte := true
	for _, r := range runes {
		if r > 0xff {
			oneByte = false
			break
		}
	}
	if oneByte {
		out = append(out, '"')
		out = binary.AppendUvarint(out, uint64(len(runes)))
		for _, r := range runes {
			out = append(out, byte(r))
		}
		return out
	}
	units := utf16.Encode(runes)
	out = append(out, 'c')
	out = binary.AppendUvarint(out, uint64(len(units)*2))
	for _, u := range units {
		out = binary.LittleEndian.AppendUint16(out, u)
	}
	return out
}

func idbString(s string) []byte {
	units := utf16.Encode([]rune(s))
	out := binary.AppendUvarint(nil, uint64(len(units)))
	for _, u := range units {
		out = binary.BigEndian.AppendUint16(out, u)
	}
	return out
}

func utf16BE(s string) []byte {
	var out []byte
	for _, u := range utf16.Encode([]rune(s)) {
		out = binary.BigEndian.AppendUint16(out, u)
	}
	return out
}

func truncatedInt(v uint64) []byte {
	if v == 0 {
		return []byte{0}
	}
	var out []byte
	for v > 0 {
		out = append(out, byte(v))
		v >>= 8
	}
	return out
}

func makePrefix(dbID, storeID, indexID uint64) []byte {
	d, s, i := truncatedInt(dbID), truncatedInt(storeID), truncatedInt(indexID)
	out := []byte{byte((len(d)-1)<<5 | (len(s)-1)<<2 | (len(i) - 1))}
	out = append(out, d...)
	out = append(out, s...)
	return append(out, i...)
}

func clone(b []byte) []byte { return append([]byte(nil), b...) }
