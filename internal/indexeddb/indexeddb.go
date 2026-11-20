// Package indexeddb reads Chromium IndexedDB databases stored in LevelDB.
//
// Ported from ccl_chromium_indexeddb.py in ccl_chromium_reader (MIT, CCL
// Forensics). Like the original, it returns every version of every record it
// finds, including old versions and deleted ones, in file order.
// Key layout reference:
// https://chromium.googlesource.com/chromium/src/+/main/content/browser/indexed_db/docs/leveldb_coding_scheme.md
package indexeddb

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"unicode/utf16"

	"github.com/stn1slv/teamscache/internal/leveldb"
)

// ErrNotFound reports a database or object store that does not exist.
var ErrNotFound = errors.New("not found")

// Metadata type bytes.
const (
	dbMetaMaxObjectStoreID = 3
	objectStoreMetaByte    = 50
	objectStoreMetaName    = 0
	databaseNamePrefixByte = 0xc9
	indexObjectStoreData   = 1
	indexBlobEntry         = 3
)

// DatabaseID identifies one database of an origin.
type DatabaseID struct {
	Number uint64
	Origin string
	Name   string
}

// DB is an IndexedDB LevelDB, read into memory.
type DB struct {
	records   []leveldb.Record
	blobDir   string
	databases []DatabaseID
	// maxStoreID[db] is the highest object store id of a database.
	maxStoreID map[uint64]uint64
	// storeNames[db][store] is the name of an object store.
	storeNames map[uint64]map[uint64]string
	blobInfo   map[blobKey]blobInfo
}

// Open reads all records of the LevelDB in dir. blobDir is the folder that
// holds values too large for LevelDB; it may be empty.
func Open(ctx context.Context, dir, blobDir string, logger *slog.Logger) (*DB, error) {
	records, err := leveldb.ReadDir(ctx, dir, logger)
	if err != nil {
		return nil, err
	}
	db := &DB{records: records, blobDir: blobDir}
	db.readMetadata()
	return db, nil
}

// Databases returns the databases in the order ccl lists them.
func (db *DB) Databases() []DatabaseID {
	return db.databases
}

// newestLive picks, for each key accepted by keyOf, the record with the
// highest sequence number, and drops keys whose newest record is a deletion.
// Records are visited newest file first, and the result keeps the order in
// which keys were first seen.
func (db *DB) newestLive(keyOf func(r *leveldb.Record) (string, bool)) ([]string, map[string]*leveldb.Record) {
	var order []string
	found := map[string]*leveldb.Record{}
	for i := len(db.records) - 1; i >= 0; i-- {
		r := &db.records[i]
		k, ok := keyOf(r)
		if !ok {
			continue
		}
		old, seen := found[k]
		if !seen {
			order = append(order, k)
		}
		if !seen || old.Seq < r.Seq {
			found[k] = r
		}
	}
	live := order[:0]
	for _, k := range order {
		if found[k].State == leveldb.Live {
			live = append(live, k)
		} else {
			delete(found, k)
		}
	}
	return live, found
}

func (db *DB) readMetadata() {
	global := []byte{0, 0, 0, 0}
	order, found := db.newestLive(func(r *leveldb.Record) (string, bool) {
		key := r.UserKey()
		return string(key), bytes.HasPrefix(key, global)
	})
	seen := map[uint64]bool{}
	for _, k := range order {
		r := found[k]
		key := r.UserKey()
		if len(key) < 5 || key[4] != databaseNamePrefixByte {
			continue
		}
		origin, n := readString(key[5:])
		if n == 0 {
			continue
		}
		name, m := readString(key[5+n:])
		if m == 0 {
			continue
		}
		number := truncatedInt(r.Value)
		if seen[number] {
			continue
		}
		seen[number] = true
		db.databases = append(db.databases, DatabaseID{Number: number, Origin: origin, Name: name})
	}

	db.maxStoreID = map[uint64]uint64{}
	db.storeNames = map[uint64]map[uint64]string{}
	for _, d := range db.databases {
		prefix := makePrefix(d.Number, 0, 0)
		_, meta := db.newestLive(func(r *leveldb.Record) (string, bool) {
			key := r.UserKey()
			if !bytes.HasPrefix(key, prefix) || len(key) <= len(prefix) {
				return "", false
			}
			return string(key[len(prefix) : len(prefix)+1]), true
		})
		if r, ok := meta[string([]byte{dbMetaMaxObjectStoreID})]; ok {
			db.maxStoreID[d.Number] = truncatedInt(r.Value)
		}

		storePrefix := append(append([]byte{}, prefix...), objectStoreMetaByte)
		names := map[uint64]string{}
		_, storeMeta := db.newestLive(func(r *leveldb.Record) (string, bool) {
			key := r.UserKey()
			if !bytes.HasPrefix(key, storePrefix) {
				return "", false
			}
			rest := key[len(storePrefix):]
			id, n := leveldb.Uvarint(rest, 10)
			if n == 0 || n >= len(rest) || rest[n] != objectStoreMetaName {
				return "", false
			}
			return strconv.FormatUint(id, 10), true
		})
		for idText, r := range storeMeta {
			id, err := strconv.ParseUint(idText, 10, 64)
			if err != nil {
				continue
			}
			if name, ok := utf16BE(r.Value); ok {
				names[id] = name
			}
		}
		db.storeNames[d.Number] = names
	}
}

// ObjectStoreID returns the lowest id of an object store with the given name,
// among the ids from 1 to the database's highest store id.
func (db *DB) ObjectStoreID(dbNumber uint64, name string) (uint64, error) {
	var best uint64
	for id, n := range db.storeNames[dbNumber] {
		if n == name && id >= 1 && id <= db.maxStoreID[dbNumber] && (best == 0 || id < best) {
			best = id
		}
	}
	if best != 0 {
		return best, nil
	}
	return 0, fmt.Errorf("object store %q in database %d: %w", name, dbNumber, ErrNotFound)
}

// makePrefix encodes a key prefix: one byte with the sizes of the three ids,
// then each id as little-endian bytes without leading zero bytes.
func makePrefix(dbID, storeID, indexID uint64) []byte {
	enc := func(v uint64) []byte {
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
	d, s, i := enc(dbID), enc(storeID), enc(indexID)
	first := byte((len(d)-1)<<5 | (len(s)-1)<<2 | (len(i) - 1))
	out := append([]byte{first}, d...)
	out = append(out, s...)
	return append(out, i...)
}

// truncatedInt decodes Chromium's EncodeInt: little-endian bytes without
// trailing zero bytes.
func truncatedInt(b []byte) uint64 {
	var v uint64
	for i := 0; i < len(b) && i < 8; i++ {
		v |= uint64(b[i]) << (8 * i)
	}
	return v
}

// readString decodes a varint length (in UTF-16 code units) followed by
// UTF-16BE text. It returns the bytes used, or 0 on bad data.
func readString(b []byte) (string, int) {
	units, n := leveldb.Uvarint(b, 10)
	if n == 0 || units > uint64(len(b)-n)/2 {
		return "", 0
	}
	end := n + int(units)*2
	s, ok := utf16BE(b[n:end])
	if !ok {
		return "", 0
	}
	return s, end
}

func utf16BE(b []byte) (string, bool) {
	if len(b)%2 != 0 {
		return "", false
	}
	units := make([]uint16, len(b)/2)
	for i := range units {
		units[i] = binary.BigEndian.Uint16(b[2*i:])
	}
	return string(utf16.Decode(units)), true
}
