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
	"log/slog"
	"slices"
	"sort"
	"unicode/utf16"

	"github.com/stn1slv/teamscache/internal/leveldb"
)

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

type storeAddr struct {
	db, store uint64
}

type indexedRecord struct {
	idx       int
	prefixLen int
}

// DB is an IndexedDB LevelDB, read into memory.
type DB struct {
	ctx       context.Context
	damage    []leveldb.Damage
	records   []leveldb.Record
	blobDir   string
	databases []DatabaseID
	// maxStoreID[db] is the highest object store id of a database.
	maxStoreID map[uint64]uint64
	// storeNames[db][store] is the name of an object store.
	storeNames map[uint64]map[uint64]string
	storeIndex map[storeAddr][]indexedRecord
	blobInfo   map[blobKey]blobInfo
}

// Open reads all records of the LevelDB in dir. blobDir is the folder that
// holds values too large for LevelDB; it may be empty.
func Open(ctx context.Context, dir, blobDir string, logger *slog.Logger) (*DB, error) {
	records, damage, err := leveldb.ReadDir(ctx, dir, logger)
	if err != nil {
		return nil, err
	}
	db := &DB{ctx: ctx, records: records, blobDir: blobDir, damage: damage}
	db.readMetadata()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return db, nil
}

// Damage returns the file sections skipped while reading LevelDB.
func (db *DB) Damage() []leveldb.Damage {
	return db.damage
}

// BlobFiles lists referenced blob files relative to the blob directory.
func (db *DB) BlobFiles() []string {
	return db.BlobFilesFor(nil)
}

// BlobFilesFor lists referenced blob files relative to the blob directory for
// stores accepted by accept. A nil accept matches all stores.
func (db *DB) BlobFilesFor(accept func(dbNumber, storeID uint64) bool) []string {
	if db.blobInfo == nil {
		db.loadBlobInfo()
	}
	seen := map[string]bool{}
	for key, info := range db.blobInfo {
		if accept != nil && !accept(key.db, key.store) {
			continue
		}
		seen[blobPath("", key.db, info.number)] = true
	}
	files := make([]string, 0, len(seen))
	for path := range seen {
		files = append(files, path)
	}
	sort.Strings(files)
	return files
}

// Databases returns the databases in the order ccl lists them.
func (db *DB) Databases() []DatabaseID {
	return db.databases
}

func (db *DB) readMetadata() {
	global := []byte{0, 0, 0, 0}
	var globalOrder []string
	globalFound := map[string]*leveldb.Record{}
	maxStoreMeta := map[uint64]*leveldb.Record{}
	storeNameMeta := map[storeAddr]*leveldb.Record{}
	storeIndex := map[storeAddr][]indexedRecord{}

	for i := len(db.records) - 1; i >= 0; i-- {
		if db.ctx != nil && db.ctx.Err() != nil {
			return
		}
		r := &db.records[i]
		key := r.UserKey()
		if bytes.HasPrefix(key, global) {
			k := string(key)
			old, seen := globalFound[k]
			if !seen {
				globalOrder = append(globalOrder, k)
			}
			if !seen || old.Seq < r.Seq {
				globalFound[k] = r
			}
			continue
		}
		dbID, storeID, indexID, n, ok := readPrefix(key)
		if !ok || dbID == 0 {
			continue
		}
		if storeID == 0 && indexID == 0 && n < len(key) {
			rest := key[n:]
			switch rest[0] {
			case dbMetaMaxObjectStoreID:
				if old, seen := maxStoreMeta[dbID]; !seen || old.Seq < r.Seq {
					maxStoreMeta[dbID] = r
				}
			case objectStoreMetaByte:
				id, m := leveldb.Uvarint(rest[1:], 10)
				if m > 0 && 1+m < len(rest) && rest[1+m] == objectStoreMetaName {
					addr := storeAddr{db: dbID, store: id}
					if old, seen := storeNameMeta[addr]; !seen || old.Seq < r.Seq {
						storeNameMeta[addr] = r
					}
				}
			}
			continue
		}
		if storeID != 0 && indexID == indexObjectStoreData {
			addr := storeAddr{db: dbID, store: storeID}
			storeIndex[addr] = append(storeIndex[addr], indexedRecord{idx: i, prefixLen: n})
		}
	}

	seenDB := map[uint64]bool{}
	for _, k := range globalOrder {
		r := globalFound[k]
		if r.State != leveldb.Live {
			continue
		}
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
		if seenDB[number] {
			continue
		}
		seenDB[number] = true
		db.databases = append(db.databases, DatabaseID{Number: number, Origin: origin, Name: name})
	}

	db.maxStoreID = make(map[uint64]uint64, len(db.databases))
	db.storeNames = make(map[uint64]map[uint64]string, len(db.databases))
	for _, d := range db.databases {
		db.storeNames[d.Number] = map[uint64]string{}
	}
	for dbID, r := range maxStoreMeta {
		if r.State == leveldb.Live {
			if _, ok := db.storeNames[dbID]; ok {
				db.maxStoreID[dbID] = truncatedInt(r.Value)
			}
		}
	}
	for addr, r := range storeNameMeta {
		if r.State != leveldb.Live {
			continue
		}
		names, ok := db.storeNames[addr.db]
		if !ok {
			continue
		}
		if name, ok := utf16BE(r.Value); ok {
			names[addr.store] = name
		}
	}
	for _, idxs := range storeIndex {
		slices.Reverse(idxs)
	}
	db.storeIndex = storeIndex
}

func (db *DB) buildStoreIndex() {
	index := map[storeAddr][]indexedRecord{}
	for i := range db.records {
		if db.ctx != nil && db.ctx.Err() != nil {
			return
		}
		dbID, storeID, indexID, n, ok := readPrefix(db.records[i].UserKey())
		if !ok || dbID == 0 || storeID == 0 || indexID != indexObjectStoreData {
			continue
		}
		addr := storeAddr{db: dbID, store: storeID}
		index[addr] = append(index[addr], indexedRecord{idx: i, prefixLen: n})
	}
	db.storeIndex = index
}

// ObjectStoreIDs returns the ids of the object stores whose name match
// accepts, in ascending order, among the ids from 1 to the database's highest
// store id. When several stores have the same name, only the lowest id counts.
func (db *DB) ObjectStoreIDs(dbNumber uint64, match func(name string) bool) []uint64 {
	lowest := map[string]uint64{}
	for id, n := range db.storeNames[dbNumber] {
		if id < 1 || id > db.maxStoreID[dbNumber] || !match(n) {
			continue
		}
		if best, ok := lowest[n]; !ok || id < best {
			lowest[n] = id
		}
	}
	ids := make([]uint64, 0, len(lowest))
	for _, id := range lowest {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
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
