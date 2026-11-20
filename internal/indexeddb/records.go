package indexeddb

import (
	"bytes"
	"errors"
	"fmt"
	"iter"
	"os"
	"path/filepath"

	"github.com/stn1slv/teamscache/internal/leveldb"
	"github.com/stn1slv/teamscache/internal/v8"
)

// ErrBadValue reports a record value that could not be decoded.
var ErrBadValue = errors.New("bad indexeddb value")

// Blink envelope constants (idb_value_wrapping.cc, trailer_reader.h).
const (
	blinkVersionTag          = 0xff
	blinkWrappedPseudoVer    = 0x11 // kRequiresProcessingSSVPseudoVersion
	blinkReplaceWithBlob     = 0x01
	blinkCompressedSnappy    = 0x02
	blinkTrailerTag          = 0xfe
	blinkTrailerSize         = 13
	blinkMinVersionNoTrailer = 21 // first wire format version with a trailer
	maxEnvelopeDepth         = 4
	maxKeyDepth              = 1000
)

// Record is one version of an object store record.
type Record struct {
	// Key is the encoded IndexedDB key.
	Key []byte
	// Value is the decoded value, or nil for an empty (deleted) value.
	Value any
	Live  bool
	Seq   uint64
	// Err is set when the value could not be decoded; Value is then nil.
	// The version is still yielded, so callers know a newer version exists.
	Err error
}

// Records yields every version of every record of an object store, in file
// order. A version whose value cannot be decoded is passed to onBad and
// yielded with Err set. A key that cannot be decoded is only passed to onBad.
func (db *DB) Records(dbNumber, storeID uint64, onBad func(error)) iter.Seq[Record] {
	prefix := makePrefix(dbNumber, storeID, indexObjectStoreData)
	return func(yield func(Record) bool) {
		for i := range db.records {
			r := &db.records[i]
			userKey := r.UserKey()
			if !bytes.HasPrefix(userKey, prefix) {
				continue
			}
			keyLen, err := idbKeyLength(userKey[len(prefix):], 0)
			if err != nil {
				onBad(err)
				continue
			}
			rec := Record{Key: userKey[len(prefix) : len(prefix)+keyLen], Live: r.State == leveldb.Live, Seq: r.Seq}
			if len(r.Value) > 0 {
				rec.Value, err = db.decodeValue(dbNumber, storeID, rec.Key, r.Value)
				if err != nil {
					rec.Err = fmt.Errorf("record in %s at offset %d: %w", filepath.Base(r.File), r.Offset, err)
					onBad(rec.Err)
				}
			}
			if !yield(rec) {
				return
			}
		}
	}
}

func (db *DB) decodeValue(dbNumber, storeID uint64, key, value []byte) (any, error) {
	// The record value starts with a varint version, then the Blink envelope.
	_, n := leveldb.Uvarint(value, 10)
	if n == 0 {
		return nil, fmt.Errorf("%w: no value version", ErrBadValue)
	}
	return db.decodeEnvelope(dbNumber, storeID, key, value[n:], 0)
}

func (db *DB) decodeEnvelope(dbNumber, storeID uint64, key, buf []byte, depth int) (any, error) {
	if depth > maxEnvelopeDepth {
		return nil, fmt.Errorf("%w: too many nested envelopes", ErrBadValue)
	}
	if len(buf) == 0 || buf[0] != blinkVersionTag {
		return nil, fmt.Errorf("%w: no Blink version tag", ErrBadValue)
	}
	version, n := leveldb.Uvarint(buf[1:], 10)
	if n == 0 {
		return nil, fmt.Errorf("%w: no Blink version", ErrBadValue)
	}
	pos := 1 + n

	if version == blinkWrappedPseudoVer {
		if pos >= len(buf) {
			return nil, fmt.Errorf("%w: wrapped value without a wrapping type", ErrBadValue)
		}
		switch buf[pos] {
		case blinkReplaceWithBlob:
			_, n1 := leveldb.Uvarint(buf[pos+1:], 10) // size
			index, n2 := leveldb.Uvarint(buf[pos+1+n1:], 10)
			if n1 == 0 || n2 == 0 {
				return nil, fmt.Errorf("%w: bad blob reference", ErrBadValue)
			}
			blob, err := db.readBlob(dbNumber, storeID, key, index)
			if err != nil {
				return nil, err
			}
			return db.decodeEnvelope(dbNumber, storeID, key, blob, depth+1)
		case blinkCompressedSnappy:
			decoded, err := leveldb.DecodeSnappy(buf[pos+1:])
			if err != nil {
				return nil, fmt.Errorf("%w: snappy: %w", ErrBadValue, err)
			}
			return db.decodeEnvelope(dbNumber, storeID, key, decoded, depth+1)
		default:
			return nil, fmt.Errorf("%w: unknown wrapping type %d", ErrBadValue, buf[pos])
		}
	}

	if version >= blinkMinVersionNoTrailer {
		if len(buf)-pos < blinkTrailerSize || buf[pos] != blinkTrailerTag {
			return nil, fmt.Errorf("%w: missing Blink trailer", ErrBadValue)
		}
		pos += blinkTrailerSize
	}
	value, err := v8.Deserialize(buf[pos:], v8.ReadBlinkHostObject)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrBadValue, err)
	}
	return value, nil
}

// idbKeyLength returns the length of the encoded IndexedDB key at the start
// of b. depth is the array nesting level of b.
func idbKeyLength(b []byte, depth int) (int, error) {
	if depth > maxKeyDepth {
		return 0, fmt.Errorf("%w: key nested too deep", ErrBadValue)
	}
	if len(b) == 0 {
		return 0, fmt.Errorf("%w: empty key", ErrBadValue)
	}
	switch b[0] {
	case 0: // null
		return 1, nil
	case 1: // string: varint length in UTF-16 code units
		units, n := leveldb.Uvarint(b[1:], 10)
		if n == 0 || units > uint64(len(b)-1-n)/2 {
			return 0, fmt.Errorf("%w: bad string key", ErrBadValue)
		}
		return 1 + n + int(units)*2, nil
	case 2, 3: // date, number: 8-byte double
		if len(b) < 9 {
			return 0, fmt.Errorf("%w: short number key", ErrBadValue)
		}
		return 9, nil
	case 4: // array
		count, n := leveldb.Uvarint(b[1:], 10)
		if n == 0 {
			return 0, fmt.Errorf("%w: bad array key", ErrBadValue)
		}
		pos := 1 + n
		for range count {
			m, err := idbKeyLength(b[pos:], depth+1)
			if err != nil {
				return 0, err
			}
			pos += m
		}
		return pos, nil
	case 6: // binary
		length, n := leveldb.Uvarint(b[1:], 10)
		if n == 0 || length > uint64(len(b)-1-n) {
			return 0, fmt.Errorf("%w: bad binary key", ErrBadValue)
		}
		return 1 + n + int(length), nil
	default:
		return 0, fmt.Errorf("%w: key type %d", ErrBadValue, b[0])
	}
}

type blobKey struct {
	db, store uint64
	key       string
	index     uint64
}

type blobInfo struct {
	number uint64
}

// readBlob returns the content of an external value.
func (db *DB) readBlob(dbNumber, storeID uint64, key []byte, index uint64) ([]byte, error) {
	if db.blobInfo == nil {
		db.loadBlobInfo()
	}
	info, ok := db.blobInfo[blobKey{db: dbNumber, store: storeID, key: string(key), index: index}]
	if !ok {
		return nil, fmt.Errorf("%w: no blob entry for an external value", ErrBadValue)
	}
	if db.blobDir == "" {
		return nil, fmt.Errorf("%w: value is stored in a blob file, but there is no blob folder", ErrBadValue)
	}
	data, err := os.ReadFile(blobPath(db.blobDir, dbNumber, info.number))
	if err != nil {
		return nil, fmt.Errorf("%w: read blob file: %w", ErrBadValue, err)
	}
	return data, nil
}

// blobPath returns the file of a blob: the database id, then the second
// lowest byte of the blob number, then the blob number, all in hexadecimal.
func blobPath(blobDir string, dbNumber, blobNumber uint64) string {
	return filepath.Join(blobDir,
		fmt.Sprintf("%x", dbNumber), fmt.Sprintf("%02x", (blobNumber&0xff00)>>8), fmt.Sprintf("%x", blobNumber))
}

// loadBlobInfo indexes the newest blob entry list of each record in one pass.
// Keys whose newest entry list is deleted have no blobs.
func (db *DB) loadBlobInfo() {
	type entryList struct {
		seq   uint64
		live  bool
		infos []blobInfo
	}
	newest := map[[3]string]entryList{}
	ids := map[[3]string][2]uint64{}
	for i := range db.records {
		r := &db.records[i]
		userKey := r.UserKey()
		dbID, storeID, indexID, n, ok := readPrefix(userKey)
		if !ok || indexID != indexBlobEntry || dbID == 0 || storeID == 0 {
			continue
		}
		k := [3]string{fmt.Sprint(dbID), fmt.Sprint(storeID), string(userKey[n:])}
		if old, seen := newest[k]; seen && old.seq > r.Seq {
			continue
		}
		newest[k] = entryList{seq: r.Seq, live: r.State == leveldb.Live, infos: parseBlobEntries(r.Value)}
		ids[k] = [2]uint64{dbID, storeID}
	}
	db.blobInfo = map[blobKey]blobInfo{}
	for k, list := range newest {
		if !list.live {
			continue
		}
		for index, info := range list.infos {
			db.blobInfo[blobKey{db: ids[k][0], store: ids[k][1], key: k[2], index: uint64(index)}] = info
		}
	}
}

// readPrefix decodes the key prefix that makePrefix writes. It returns the
// three ids and the prefix length.
func readPrefix(b []byte) (dbID, storeID, indexID uint64, n int, ok bool) {
	if len(b) == 0 {
		return 0, 0, 0, 0, false
	}
	sizes := [3]int{int(b[0]>>5&0x07) + 1, int(b[0]>>2&0x07) + 1, int(b[0]&0x03) + 1}
	var ids [3]uint64
	pos := 1
	for i, size := range sizes {
		if len(b) < pos+size {
			return 0, 0, 0, 0, false
		}
		for j := 0; j < size; j++ {
			ids[i] |= uint64(b[pos+j]) << (8 * j)
		}
		pos += size
	}
	return ids[0], ids[1], ids[2], pos, true
}

// parseBlobEntries decodes a list of external objects. It stops at the first
// entry it cannot decode.
func parseBlobEntries(b []byte) []blobInfo {
	var out []blobInfo
	pos := 0
	next := func() (uint64, bool) {
		v, n := leveldb.Uvarint(b[pos:], 10)
		pos += n
		return v, n > 0
	}
	skipString := func() bool {
		units, ok := next()
		if !ok || units > uint64(len(b)-pos)/2 {
			return false
		}
		pos += int(units) * 2
		return true
	}
	for pos < len(b) {
		kind := b[pos]
		pos++
		if kind != 0 && kind != 1 { // only Blob (0) and File (1) are supported
			return out
		}
		number, ok := next()
		if !ok || !skipString() { // mime type
			return out
		}
		if _, ok := next(); !ok { // size
			return out
		}
		if kind == 1 {
			if !skipString() { // file name
				return out
			}
			if _, ok := next(); !ok { // last modified
				return out
			}
		}
		out = append(out, blobInfo{number: number})
	}
	return out
}
