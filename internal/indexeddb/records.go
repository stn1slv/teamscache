package indexeddb

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"iter"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/golang/snappy"

	"github.com/stn1slv/teamscache/internal/leveldb"
	"github.com/stn1slv/teamscache/internal/v8"
)

// ErrBadValue reports a record value that could not be decoded.
var ErrBadValue = errors.New("bad indexeddb value")

// Blink envelope constants (idb_value_wrapping.cc, trailer_reader.h).
const (
	blinkVersionTag           = 0xff
	blinkWrappedPseudoVer     = 0x11 // kRequiresProcessingSSVPseudoVersion
	blinkReplaceWithBlob      = 0x01
	blinkCompressedSnappy     = 0x02
	blinkTrailerTag           = 0xfe
	blinkTrailerSize          = 13
	blinkMinVersionNoTrailer  = 21 // first wire format version with a trailer
	maxEnvelopeDepth          = 4
	maxKeyDepth               = 1000
	parallelDecodeThreshold   = 16
	parallelDecodeWindowSize  = 256
	parallelDecodeWindowBytes = 16 << 20
)

// External object types (indexed_db_external_object.h).
const (
	externalObjectBlob             = 0
	externalObjectFile             = 1
	externalObjectFileSystemHandle = 2
)

// Record is one version of an object store record.
type Record struct {
	// Key is the encoded IndexedDB key.
	Key []byte
	// Value is the decoded value, or nil for an empty (deleted) value.
	Value  any
	Live   bool
	Seq    uint64
	File   string
	Offset int64
	// Err is set when the value could not be decoded; Value is then nil.
	// The version is still yielded, so callers know a newer version exists.
	Err error
}

// ensureStoreLoaded loads the store on demand when db was opened lazily.
// A non-cancellation load failure is remembered per store so repeated reads do
// not retry or report the same error again.
func (db *DB) ensureStoreLoaded(dbNumber, storeID uint64, onBad func(error)) bool {
	if !db.lazyStores {
		return true
	}
	addr := storeAddr{db: dbNumber, store: storeID}
	db.mu.RLock()
	loaded := db.loadedStores[addr]
	failed := db.failedStores[addr] != nil
	db.mu.RUnlock()
	if loaded {
		return true
	}
	if failed {
		return false
	}
	if err := db.LoadStores(db.ctx, func(d, s uint64) bool { return d == dbNumber && s == storeID }); err != nil {
		if db.ctxErr() == nil {
			db.mu.Lock()
			if db.failedStores == nil {
				db.failedStores = map[storeAddr]error{}
			}
			firstFailure := db.failedStores[addr] == nil
			db.failedStores[addr] = err
			db.addDamageLocked([]leveldb.Damage{{
				File:   fmt.Sprintf("store %d/%d", dbNumber, storeID),
				Reason: fmt.Sprintf("load store failed: %v", err),
			}})
			db.mu.Unlock()
			if firstFailure && onBad != nil {
				onBad(err)
			}
		}
		return false
	}
	return true
}

func (db *DB) snapshotStore(addr storeAddr) ([]indexedRecord, []leveldb.Record) {
	db.mu.RLock()
	if db.storeIndex != nil {
		entries := slices.Clone(db.storeIndex[addr])
		recs := db.records
		db.mu.RUnlock()
		return entries, recs
	}
	db.mu.RUnlock()

	db.mu.Lock()
	if db.storeIndex == nil {
		db.buildStoreIndex()
	}
	entries := slices.Clone(db.storeIndex[addr])
	recs := db.records
	db.mu.Unlock()
	return entries, recs
}

// Records yields every version of every record of an object store, in file
// order. A version whose value cannot be decoded is passed to onBad and
// yielded with Err set. A key that cannot be decoded is only passed to onBad.
// Cancellation follows the context passed to Open or OpenLive.
func (db *DB) Records(dbNumber, storeID uint64, onBad func(error)) iter.Seq[Record] {
	if onBad == nil {
		onBad = func(error) {}
	}
	return func(yield func(Record) bool) {
		if !db.ensureStoreLoaded(dbNumber, storeID, onBad) {
			return
		}
		entries, allRecords := db.snapshotStore(storeAddr{db: dbNumber, store: storeID})
		for _, entry := range entries {
			if db.ctxErr() != nil {
				return
			}
			r := &allRecords[entry.idx]
			userKey := r.UserKey()
			if len(userKey) < entry.prefixLen {
				onBad(recordError(r, fmt.Errorf("%w: truncated record key", ErrBadValue)))
				continue
			}
			keyLen, err := idbKeyLength(userKey[entry.prefixLen:], 0)
			if err != nil {
				onBad(recordError(r, err))
				continue
			}
			rec := db.decodeRecordSafe(dbNumber, storeID, r, userKey[entry.prefixLen:entry.prefixLen+keyLen])
			if db.ctxErr() != nil {
				return
			}
			if rec.Err != nil {
				onBad(rec.Err)
			}
			if !yield(rec) {
				return
			}
		}
	}
}

// CurrentRecords yields only the newest live version of each key in an object
// store, in the order each key first appeared. Keys whose newest version is a
// deletion are omitted without decoding older versions. A winning live version
// whose value cannot be decoded is passed to onBad and yielded with Err set.
// Cancellation follows the context passed to Open or OpenLive.
func (db *DB) CurrentRecords(dbNumber, storeID uint64, onBad func(error)) iter.Seq[Record] {
	if onBad == nil {
		onBad = func(error) {}
	}
	return func(yield func(Record) bool) {
		if !db.ensureStoreLoaded(dbNumber, storeID, onBad) {
			return
		}
		entries, allRecords := db.snapshotStore(storeAddr{db: dbNumber, store: storeID})
		type candidate struct {
			rec *leveldb.Record
			key []byte
		}
		var order []string
		newest := make(map[string]candidate, len(entries))
		for _, entry := range entries {
			if db.ctxErr() != nil {
				return
			}
			r := &allRecords[entry.idx]
			userKey := r.UserKey()
			if len(userKey) < entry.prefixLen {
				onBad(recordError(r, fmt.Errorf("%w: truncated record key", ErrBadValue)))
				continue
			}
			keyLen, err := idbKeyLength(userKey[entry.prefixLen:], 0)
			if err != nil {
				onBad(recordError(r, err))
				continue
			}
			keyBytes := userKey[entry.prefixLen : entry.prefixLen+keyLen]
			ks := string(keyBytes)
			old, seen := newest[ks]
			if !seen {
				order = append(order, ks)
			}
			if !seen || r.Seq >= old.rec.Seq {
				newest[ks] = candidate{rec: r, key: keyBytes}
			}
		}

		live := make([]candidate, 0, len(order))
		for _, ks := range order {
			if c := newest[ks]; c.rec.State == leveldb.Live {
				live = append(live, c)
			}
		}
		if len(live) == 0 || db.ctxErr() != nil {
			return
		}

		if len(live) < parallelDecodeThreshold || runtime.GOMAXPROCS(0) < 2 {
			for _, c := range live {
				if db.ctxErr() != nil {
					return
				}
				rec := db.decodeRecordSafe(dbNumber, storeID, c.rec, c.key)
				if db.ctxErr() != nil {
					return
				}
				if rec.Err != nil {
					onBad(rec.Err)
				}
				if !yield(rec) {
					return
				}
			}
			return
		}

		// Preload blob metadata before workers start so workers only take read locks.
		db.ensureBlobInfo()
		if db.ctxErr() != nil {
			return
		}
		// Decode in windows bounded by both record count and estimated byte size so
		// peak memory stays bounded and callers that stop early avoid extra work.
		decoded := make([]Record, min(parallelDecodeWindowSize, len(live)))
		for start := 0; start < len(live); {
			if db.ctxErr() != nil {
				return
			}
			end := start
			var windowBytes int
			for end < len(live) && end-start < parallelDecodeWindowSize {
				est := estimatedRecordBytes(live[end].rec.Value)
				if end > start && windowBytes+est > parallelDecodeWindowBytes {
					break
				}
				windowBytes += est
				end++
			}
			window := live[start:end]
			start = end
			batch := decoded[:len(window)]
			workers := min(runtime.GOMAXPROCS(0), len(window))
			var next atomic.Int64
			var wg sync.WaitGroup
			for range workers {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for {
						i := int(next.Add(1) - 1)
						if i >= len(window) || db.ctxErr() != nil {
							return
						}
						batch[i] = db.decodeRecordSafe(dbNumber, storeID, window[i].rec, window[i].key)
					}
				}()
			}
			wg.Wait()
			for _, rec := range batch {
				if db.ctxErr() != nil {
					return
				}
				if rec.Err != nil {
					onBad(rec.Err)
				}
				if !yield(rec) {
					return
				}
			}
		}
	}
}

// estimatedRecordBytes returns an approximate decoded byte cost for budgeting
// a parallel decode window, accounting for declared Snappy expansion and
// external blob sizes.
func estimatedRecordBytes(value []byte) int {
	est := len(value)
	_, n := leveldb.Uvarint(value, 10)
	if n == 0 || n >= len(value) {
		return max(est, 1)
	}
	buf := value[n:]
	if len(buf) < 3 || buf[0] != blinkVersionTag {
		return max(est, 1)
	}
	ver, m := leveldb.Uvarint(buf[1:], 10)
	if m == 0 || ver != blinkWrappedPseudoVer || 1+m >= len(buf) {
		return max(est, 1)
	}
	switch buf[1+m] {
	case blinkReplaceWithBlob:
		if blobSize, k := leveldb.Uvarint(buf[2+m:], 10); k > 0 {
			est += int(min(blobSize, uint64(parallelDecodeWindowBytes)))
		}
	case blinkCompressedSnappy:
		if decodedLen, err := snappy.DecodedLen(buf[2+m:]); err == nil && decodedLen > 0 {
			est += min(decodedLen, parallelDecodeWindowBytes)
		}
	}
	// Account for Go object allocation overhead beyond raw serialized bytes.
	return min(max(est*2, 1), parallelDecodeWindowBytes)
}

func (db *DB) decodeRecordSafe(dbNumber, storeID uint64, r *leveldb.Record, key []byte) (rec Record) {
	defer func() {
		if p := recover(); p != nil {
			if db.logger != nil {
				db.logger.Error("panic while decoding indexeddb record",
					"file", filepath.Base(r.File), "offset", r.Offset, "panic", p, "stack", string(debug.Stack()))
			}
			rec = Record{
				Key:    key,
				Live:   r.State == leveldb.Live,
				Seq:    r.Seq,
				File:   filepath.Base(r.File),
				Offset: r.Offset,
				Err:    recordError(r, fmt.Errorf("%w: panic while decoding: %v", ErrBadValue, p)),
			}
		}
	}()
	return db.decodeRecord(dbNumber, storeID, r, key)
}

func recordError(r *leveldb.Record, err error) error {
	return fmt.Errorf("record in %s at offset %d: %w", filepath.Base(r.File), r.Offset, err)
}

func (db *DB) decodeRecord(dbNumber, storeID uint64, r *leveldb.Record, key []byte) Record {
	rec := Record{
		Key:    key,
		Live:   r.State == leveldb.Live,
		Seq:    r.Seq,
		File:   filepath.Base(r.File),
		Offset: r.Offset,
	}
	if len(r.Value) > 0 {
		val, err := db.decodeValue(dbNumber, storeID, rec.Key, r.Value)
		if err != nil {
			if db.ctxErr() != nil {
				return Record{}
			}
			rec.Err = recordError(r, err)
		} else {
			rec.Value = val
		}
	}
	return rec
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
	if err := db.ctxErr(); err != nil {
		return nil, err
	}
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
			size, n1 := leveldb.Uvarint(buf[pos+1:], 10)
			index, n2 := leveldb.Uvarint(buf[pos+1+n1:], 10)
			if n1 == 0 || n2 == 0 {
				return nil, fmt.Errorf("%w: bad blob reference", ErrBadValue)
			}
			if size > leveldb.MaxDecodedSize {
				return nil, fmt.Errorf("%w: blob declares %d bytes", ErrBadValue, size)
			}
			blob, err := db.readBlob(dbNumber, storeID, key, index, size)
			if err != nil {
				return nil, err
			}
			if uint64(len(blob)) != size {
				return nil, fmt.Errorf("%w: blob has %d bytes, reference declares %d", ErrBadValue, len(blob), size)
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

	v8End := len(buf)
	if version >= blinkMinVersionNoTrailer {
		if len(buf)-pos < blinkTrailerSize || buf[pos] != blinkTrailerTag {
			return nil, fmt.Errorf("%w: missing Blink trailer", ErrBadValue)
		}
		trailerOffset := binary.BigEndian.Uint64(buf[pos+1 : pos+9])
		trailerLen := uint64(binary.BigEndian.Uint32(buf[pos+9 : pos+13]))
		pos += blinkTrailerSize
		if trailerOffset != 0 || trailerLen != 0 {
			if trailerOffset < uint64(pos) || trailerOffset > uint64(len(buf)) || trailerLen > uint64(len(buf))-trailerOffset {
				return nil, fmt.Errorf("%w: Blink trailer out of bounds (offset %d, length %d, size %d)", ErrBadValue, trailerOffset, trailerLen, len(buf))
			}
			v8End = int(trailerOffset)
		}
	}
	v8Ctx := db.ctx
	if v8Ctx == nil {
		v8Ctx = context.Background()
	}
	value, err := v8.Deserialize(v8Ctx, buf[pos:v8End], v8.ReadBlinkHostObject)
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

type recordKey struct {
	db, store uint64
	key       string
}

type blobInfo struct {
	kind   byte
	number uint64
}

type blobEntry struct {
	infos []blobInfo
	err   error
}

type parsedBlobRecord struct {
	seq   uint64
	live  bool
	entry blobEntry
}

// readBlob returns the content of an external value after checking that the
// file size on disk matches expectedSize and stays within MaxDecodedSize.
func (db *DB) readBlob(dbNumber, storeID uint64, key []byte, index, expectedSize uint64) ([]byte, error) {
	db.ensureBlobInfo()
	db.mu.RLock()
	entry, ok := db.blobEntries[recordKey{db: dbNumber, store: storeID, key: string(key)}]
	db.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("%w: no blob entry for an external value", ErrBadValue)
	}
	if index >= uint64(len(entry.infos)) {
		if entry.err != nil {
			return nil, entry.err
		}
		return nil, fmt.Errorf("%w: no blob entry for an external value", ErrBadValue)
	}
	info := entry.infos[index]
	if info.kind != externalObjectBlob && info.kind != externalObjectFile {
		return nil, fmt.Errorf("%w: external object at index %d has unsupported type %d", ErrBadValue, index, info.kind)
	}
	if db.blobDir == "" {
		return nil, fmt.Errorf("%w: value is stored in a blob file, but there is no blob folder", ErrBadValue)
	}
	file, err := os.Open(blobPath(db.blobDir, dbNumber, info.number))
	if err != nil {
		return nil, fmt.Errorf("%w: read blob file: %w", ErrBadValue, err)
	}
	defer func() { _ = file.Close() }() // read-only, the read error is what matters
	infoOnDisk, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("%w: stat blob file: %w", ErrBadValue, err)
	}
	if infoOnDisk.Size() > leveldb.MaxDecodedSize {
		return nil, fmt.Errorf("%w: blob file exceeds %d bytes", ErrBadValue, leveldb.MaxDecodedSize)
	}
	if infoOnDisk.Size() < 0 || uint64(infoOnDisk.Size()) != expectedSize {
		return nil, fmt.Errorf("%w: blob has %d bytes, reference declares %d", ErrBadValue, infoOnDisk.Size(), expectedSize)
	}
	readCtx := db.ctx
	if readCtx == nil {
		readCtx = context.Background()
	}
	limit := min(int64(expectedSize), int64(leveldb.MaxDecodedSize)) + 1
	data, err := io.ReadAll(io.LimitReader(leveldb.ContextReader{Context: readCtx, Reader: file}, limit))
	if err != nil {
		return nil, fmt.Errorf("%w: read blob file: %w", ErrBadValue, err)
	}
	if len(data) > leveldb.MaxDecodedSize {
		return nil, fmt.Errorf("%w: blob file exceeds %d bytes", ErrBadValue, leveldb.MaxDecodedSize)
	}
	return data, nil
}

// blobPath returns the file of a blob: the database id, then the second
// lowest byte of the blob number, then the blob number, all in hexadecimal.
func blobPath(blobDir string, dbNumber, blobNumber uint64) string {
	return filepath.Join(blobDir,
		fmt.Sprintf("%x", dbNumber), fmt.Sprintf("%02x", (blobNumber&0xff00)>>8), fmt.Sprintf("%x", blobNumber))
}

func (db *DB) ensureBlobInfo() {
	db.mu.RLock()
	ready := db.blobEntries != nil
	db.mu.RUnlock()
	if ready {
		return
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.blobEntries == nil {
		db.loadBlobInfoLocked()
	}
}

// loadBlobInfo indexes the newest blob entry list of each record in one pass.
// Keys whose newest entry list is deleted have no blobs.
func (db *DB) loadBlobInfo() {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.loadBlobInfoLocked()
}

func (db *DB) loadBlobInfoLocked() {
	parsed, err := collectBlobEntries(db.ctx, db.records)
	if err != nil {
		return
	}
	db.blobEntries = map[recordKey]blobEntry{}
	db.blobSeq = map[recordKey]uint64{}
	db.mergeBlobEntriesLocked(parsed)
}

func collectBlobEntries(ctx context.Context, records []leveldb.Record) (map[recordKey]parsedBlobRecord, error) {
	type entryList struct {
		seq  uint64
		live bool
		raw  []byte
	}
	newest := map[recordKey]entryList{}
	for i := range records {
		if ctx != nil {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		r := &records[i]
		userKey := r.UserKey()
		dbID, storeID, indexID, n, ok := readPrefix(userKey)
		if !ok || indexID != indexBlobEntry || dbID == 0 || storeID == 0 {
			continue
		}
		k := recordKey{db: dbID, store: storeID, key: string(userKey[n:])}
		if old, seen := newest[k]; seen && old.seq > r.Seq {
			continue
		}
		newest[k] = entryList{seq: r.Seq, live: r.State == leveldb.Live, raw: r.Value}
	}
	parsed := make(map[recordKey]parsedBlobRecord, len(newest))
	for k, list := range newest {
		if ctx != nil {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		rec := parsedBlobRecord{seq: list.seq, live: list.live}
		if list.live {
			infos, err := parseBlobEntries(list.raw)
			rec.entry = blobEntry{infos: infos, err: err}
		}
		parsed[k] = rec
	}
	return parsed, nil
}

func (db *DB) mergeBlobEntriesLocked(parsed map[recordKey]parsedBlobRecord) {
	for k, rec := range parsed {
		if prevSeq, exists := db.blobSeq[k]; exists && prevSeq > rec.seq {
			continue
		}
		db.blobSeq[k] = rec.seq
		if !rec.live {
			delete(db.blobEntries, k)
			continue
		}
		db.blobEntries[k] = rec.entry
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

// parseBlobEntries decodes a list of external objects. It returns any entries
// parsed before an error alongside a non-nil error if the list is truncated or
// contains an unknown external object type.
func parseBlobEntries(b []byte) ([]blobInfo, error) {
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
		switch kind {
		case externalObjectBlob, externalObjectFile:
			number, ok := next()
			if !ok || !skipString() { // mime type
				return out, fmt.Errorf("%w: truncated external object entry (type %d)", ErrBadValue, kind)
			}
			if _, ok := next(); !ok { // size
				return out, fmt.Errorf("%w: truncated external object size (type %d)", ErrBadValue, kind)
			}
			if kind == externalObjectFile {
				if !skipString() { // file name
					return out, fmt.Errorf("%w: truncated external file name", ErrBadValue)
				}
				if _, ok := next(); !ok { // last modified
					return out, fmt.Errorf("%w: truncated external file timestamp", ErrBadValue)
				}
			}
			out = append(out, blobInfo{kind: kind, number: number})
		case externalObjectFileSystemHandle:
			tokenLen, ok := next()
			if !ok || tokenLen > uint64(len(b)-pos) {
				return out, fmt.Errorf("%w: truncated file system handle external object", ErrBadValue)
			}
			pos += int(tokenLen)
			// Keep a slot so later blob indices stay aligned.
			out = append(out, blobInfo{kind: kind})
		default:
			return out, fmt.Errorf("%w: unknown external object type %d", ErrBadValue, kind)
		}
	}
	return out, nil
}
