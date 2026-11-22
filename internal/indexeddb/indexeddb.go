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
	"cmp"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"path/filepath"
	"slices"
	"sort"
	"sync"
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
	idbComparatorName      = "idb_cmp1"
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

// DB is an IndexedDB LevelDB, read into memory. The context passed to Open or
// OpenLive governs cancellation for all subsequent reads on the DB.
// Internal synchronization protects lazy store loads and map reads so DB
// methods can safely overlap, though callers normally use a DB sequentially.
type DB struct {
	ctx          context.Context
	dir          string
	logger       *slog.Logger
	manifest     *leveldb.Manifest
	lazyStores   bool
	loadSemOnce  sync.Once
	loadSem      chan struct{}
	mu           sync.RWMutex
	loadedStores map[storeAddr]bool
	failedStores map[storeAddr]error
	logRecords   []leveldb.Record
	damage       []leveldb.Damage
	damageSeen   map[leveldb.Damage]bool
	records      []leveldb.Record
	blobDir      string
	databases    []DatabaseID
	// maxStoreID[db] is the highest object store id of a database.
	maxStoreID map[uint64]uint64
	// storeNames[db][store] is the name of an object store.
	storeNames  map[uint64]map[uint64]string
	storeIndex  map[storeAddr][]indexedRecord
	blobEntries map[recordKey]blobEntry
	blobSeq     map[recordKey]uint64
}

func (db *DB) ctxErr() error {
	if db.ctx == nil {
		return nil
	}
	return db.ctx.Err()
}

func (db *DB) acquireLoadLock(ctx context.Context) error {
	db.loadSemOnce.Do(func() {
		db.loadSem = make(chan struct{}, 1)
	})
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	select {
	case db.loadSem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (db *DB) releaseLoadLock() {
	<-db.loadSem
}

// mergeContext returns a non-nil context that is canceled synchronously if
// either ctx or other is already canceled, or as soon as either cancels later.
func mergeContext(ctx, other context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil && other == nil {
		return context.Background(), func() {}
	}
	if ctx == nil {
		return other, func() {}
	}
	if other == nil || other == ctx || other.Done() == nil {
		return ctx, func() {}
	}
	if ctx.Err() != nil {
		return ctx, func() {}
	}
	if other.Err() != nil {
		return other, func() {}
	}
	merged, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(other, cancel)
	if other.Err() != nil {
		cancel()
	}
	return merged, func() {
		stop()
		cancel()
	}
}

// Open reads all records of the LevelDB in dir. blobDir is the folder that
// holds values too large for LevelDB; it may be empty. The provided ctx governs
// cancellation for Open and subsequent reads on the returned DB.
func Open(ctx context.Context, dir, blobDir string, logger *slog.Logger) (*DB, error) {
	records, damage, err := leveldb.ReadDir(ctx, dir, logger)
	if err != nil {
		return nil, err
	}
	db := &DB{ctx: ctx, dir: dir, logger: logger, records: records, blobDir: blobDir}
	db.addDamageLocked(damage)
	db.readMetadata()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return db, nil
}

// OpenLive opens dir using CURRENT and the active MANIFEST when available,
// skipping obsolete table and log files. For Chromium idb_cmp1 stores, it
// reads only metadata (and caches active log records) on OpenLive, then loads
// object stores on demand via LoadStores, BlobFiles/BlobFilesFor, or
// Records/CurrentRecords; callers using lazy loading should pass an immutable
// snapshot directory. When CURRENT or the manifest is missing, unreadable, or
// references missing or size-mismatched files, it falls back to reading all
// data files. The provided ctx governs cancellation for OpenLive and subsequent
// reads on the returned DB.
func OpenLive(ctx context.Context, dir, blobDir string, logger *slog.Logger) (*DB, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	manifest, err := leveldb.ReadManifest(ctx, dir)
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if err != nil {
		db, openErr := Open(ctx, dir, blobDir, logger)
		if openErr != nil {
			return nil, openErr
		}
		if !errors.Is(err, fs.ErrNotExist) {
			logger.Warn("manifest unreadable; fell back to scanning all leveldb files", "dir", dir, "error", err)
			db.addDamage([]leveldb.Damage{{File: "CURRENT", Reason: err.Error()}})
		}
		return db, nil
	}

	problems, err := leveldb.ValidateManifestDir(dir, manifest)
	if err != nil {
		return nil, err
	}
	if len(problems) > 0 {
		db, openErr := Open(ctx, dir, blobDir, logger)
		if openErr != nil {
			return nil, openErr
		}
		for _, p := range problems {
			logger.Warn("manifest file check failed; fell back to scanning all leveldb files", "dir", dir, "file", p.File, "reason", p.Reason)
		}
		db.addDamage(problems)
		return db, nil
	}

	if manifest.Comparator == idbComparatorName {
		// Read active logs once so subsequent LoadStores calls never reread them.
		logRecords, logDamage, err := leveldb.ReadDirOptions(ctx, dir, logger, leveldb.ReadOptions{
			KeepFile: func(number uint64, isLog bool) bool {
				return isLog && manifest.IsActiveLog(number)
			},
		})
		if err != nil {
			return nil, err
		}
		tableOpts := leveldb.ReadOptions{
			KeepFile: func(number uint64, isLog bool) bool {
				if isLog {
					return false
				}
				ti, ok := manifest.Tables[number]
				return ok && overlapsMetadata(ti.SmallestUserKey(), ti.LargestUserKey())
			},
			KeepBlock:  overlapsMetadata,
			KeepRecord: isMetadataKey,
		}
		metaRecords, tableDamage, err := leveldb.ReadDirOptions(ctx, dir, logger, tableOpts)
		if err != nil {
			return nil, err
		}
		for i := range logRecords {
			if isMetadataKey(logRecords[i].UserKey()) {
				metaRecords = append(metaRecords, logRecords[i])
			}
		}
		db := &DB{
			ctx:          ctx,
			dir:          dir,
			logger:       logger,
			manifest:     manifest,
			lazyStores:   true,
			loadedStores: map[storeAddr]bool{},
			failedStores: map[storeAddr]error{},
			logRecords:   logRecords,
			records:      metaRecords,
			blobDir:      blobDir,
		}
		db.addDamageLocked(tableDamage)
		db.addDamageLocked(logDamage)
		db.readMetadata()
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return db, nil
	}

	opts := leveldb.ReadOptions{
		KeepFile: func(number uint64, isLog bool) bool {
			if isLog {
				return manifest.IsActiveLog(number)
			}
			return manifest.IsActiveTable(number)
		},
	}
	records, damage, err := leveldb.ReadDirOptions(ctx, dir, logger, opts)
	if err != nil {
		return nil, err
	}
	db := &DB{ctx: ctx, dir: dir, logger: logger, manifest: manifest, records: records, blobDir: blobDir}
	db.addDamageLocked(damage)
	db.readMetadata()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return db, nil
}

// LoadStores loads object store data and blob entries for unloaded stores
// accepted by accept. A nil accept loads all stores. When the database was not
// opened lazily, LoadStores is a no-op. Cancellation observes both ctx and the
// context passed to OpenLive.
func (db *DB) LoadStores(ctx context.Context, accept func(dbNumber, storeID uint64) bool) error {
	loadCtx, cancel := mergeContext(ctx, db.ctx)
	defer cancel()
	if err := loadCtx.Err(); err != nil {
		return err
	}
	if !db.lazyStores {
		return nil
	}

	// Snapshot store metadata and evaluate caller callback outside internal locks
	// so reentrant DB calls inside accept cannot deadlock.
	db.mu.RLock()
	var candidates []storeAddr
	for _, d := range db.databases {
		maxID := db.maxStoreID[d.Number]
		for storeID := range db.storeNames[d.Number] {
			if storeID < 1 || storeID > maxID {
				continue
			}
			candidates = append(candidates, storeAddr{db: d.Number, store: storeID})
		}
	}
	db.mu.RUnlock()

	accepted := make([]storeAddr, 0, len(candidates))
	for _, addr := range candidates {
		if accept == nil || accept(addr.db, addr.store) {
			accepted = append(accepted, addr)
		}
	}
	if len(accepted) == 0 {
		return loadCtx.Err()
	}

	if err := db.acquireLoadLock(loadCtx); err != nil {
		return err
	}
	defer db.releaseLoadLock()

	db.mu.RLock()
	needed := map[storeAddr]bool{}
	var targets []storeAddr
	for _, addr := range accepted {
		if db.loadedStores[addr] {
			continue
		}
		needed[addr] = true
		targets = append(targets, addr)
	}
	db.mu.RUnlock()
	if len(targets) == 0 {
		return loadCtx.Err()
	}
	slices.SortFunc(targets, func(a, b storeAddr) int {
		if c := cmp.Compare(a.db, b.db); c != 0 {
			return c
		}
		return cmp.Compare(a.store, b.store)
	})

	expectedTables := map[uint64]bool{}
	for num, ti := range db.manifest.Tables {
		if overlapsStores(ti.SmallestUserKey(), ti.LargestUserKey(), targets) {
			expectedTables[num] = true
		}
	}
	// KeepFile runs sequentially on the calling goroutine before worker goroutines start.
	seenTables := make(map[uint64]bool, len(expectedTables))

	matchRecord := func(userKey []byte) bool {
		dbID, storeID, indexID, _, ok := readPrefix(userKey)
		if !ok || (indexID != indexObjectStoreData && indexID != indexBlobEntry) {
			return false
		}
		return needed[storeAddr{db: dbID, store: storeID}]
	}
	opts := leveldb.ReadOptions{
		KeepFile: func(number uint64, isLog bool) bool {
			if isLog {
				return false
			}
			if expectedTables[number] {
				seenTables[number] = true
				return true
			}
			return false
		},
		KeepBlock: func(prevUserKey, userKey []byte) bool {
			return overlapsStores(prevUserKey, userKey, targets)
		},
		KeepRecord: matchRecord,
	}
	records, damage, err := leveldb.ReadDirOptions(loadCtx, db.dir, db.logger, opts)
	if err != nil {
		if loadCtx.Err() != nil {
			return loadCtx.Err()
		}
		if errors.Is(err, fs.ErrNotExist) {
			db.addDamage([]leveldb.Damage{{
				File:   filepath.Base(db.dir),
				Reason: err.Error(),
			}})
		}
		return err
	}
	if len(seenTables) < len(expectedTables) {
		var missing []uint64
		for num := range expectedTables {
			if !seenTables[num] {
				missing = append(missing, num)
			}
		}
		slices.Sort(missing)
		sections := make([]leveldb.Damage, 0, len(missing))
		for _, num := range missing {
			sections = append(sections, leveldb.Damage{
				File:   fmt.Sprintf("%06d.ldb", num),
				Reason: "manifest table missing during store load",
			})
		}
		db.addDamage(sections)
		return fmt.Errorf("%w: manifest table %06d.ldb missing during store load", leveldb.ErrCorrupt, missing[0])
	}
	for i := range db.logRecords {
		if err := loadCtx.Err(); err != nil {
			return err
		}
		if matchRecord(db.logRecords[i].UserKey()) {
			records = append(records, db.logRecords[i])
		}
	}
	if err := loadCtx.Err(); err != nil {
		return err
	}

	// Precompute blob updates before locking so cancellation leaves DB unchanged.
	parsedBlobs, err := collectBlobEntries(loadCtx, records)
	if err != nil {
		return err
	}

	db.mu.Lock()
	defer db.mu.Unlock()
	if err := loadCtx.Err(); err != nil {
		return err
	}
	db.addDamageLocked(damage)
	if db.storeIndex == nil {
		db.storeIndex = map[storeAddr][]indexedRecord{}
	}
	base := len(db.records)
	db.records = append(db.records, records...)
	for i := range records {
		dbID, storeID, indexID, n, ok := readPrefix(records[i].UserKey())
		if !ok || indexID != indexObjectStoreData {
			continue
		}
		addr := storeAddr{db: dbID, store: storeID}
		db.storeIndex[addr] = append(db.storeIndex[addr], indexedRecord{idx: base + i, prefixLen: n})
	}
	if db.blobEntries != nil {
		db.mergeBlobEntriesLocked(parsedBlobs)
	}
	for _, addr := range targets {
		db.loadedStores[addr] = true
		delete(db.failedStores, addr)
	}
	return nil
}

func (db *DB) addDamage(sections []leveldb.Damage) {
	if len(sections) == 0 {
		return
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.addDamageLocked(sections)
}

func (db *DB) addDamageLocked(sections []leveldb.Damage) {
	if len(sections) == 0 {
		return
	}
	if db.damageSeen == nil {
		db.damageSeen = make(map[leveldb.Damage]bool, len(sections))
	}
	for _, d := range sections {
		if db.damageSeen[d] {
			continue
		}
		db.damageSeen[d] = true
		db.damage = append(db.damage, d)
	}
}

type keyPrefix struct {
	db, store, index uint64
}

func comparePrefix(a, b keyPrefix) int {
	if c := cmp.Compare(a.db, b.db); c != 0 {
		return c
	}
	if c := cmp.Compare(a.store, b.store); c != 0 {
		return c
	}
	return cmp.Compare(a.index, b.index)
}

func parseKeyPrefix(b []byte) (keyPrefix, bool) {
	dbID, storeID, indexID, _, ok := readPrefix(b)
	return keyPrefix{db: dbID, store: storeID, index: indexID}, ok
}

func isMetadataKey(userKey []byte) bool {
	if bytes.HasPrefix(userKey, []byte{0, 0, 0, 0}) {
		return true
	}
	dbID, storeID, _, _, ok := readPrefix(userKey)
	return ok && (dbID == 0 || storeID == 0)
}

// overlapsMetadata reports whether an idb_cmp1 key range [lo, hi] can contain
// any global or database metadata key (db == 0 or store == 0).
func overlapsMetadata(loKey, hiKey []byte) bool {
	if len(loKey) == 0 || len(hiKey) == 0 {
		return true
	}
	lo, okLo := parseKeyPrefix(loKey)
	hi, okHi := parseKeyPrefix(hiKey)
	if !okLo || !okHi {
		return true
	}
	if lo.db == 0 || lo.store == 0 || hi.db > lo.db {
		return true
	}
	return false
}

// overlapsStores reports whether an idb_cmp1 key range [lo, hi] can contain
// object store data (index 1) or blob entries (index 3) for any target store.
func overlapsStores(loKey, hiKey []byte, targets []storeAddr) bool {
	if len(targets) == 0 {
		return false
	}
	var lo, hi keyPrefix
	hasLo, hasHi := false, false
	if len(loKey) > 0 {
		var ok bool
		lo, ok = parseKeyPrefix(loKey)
		if !ok {
			return true
		}
		hasLo = true
	}
	if len(hiKey) > 0 {
		var ok bool
		hi, ok = parseKeyPrefix(hiKey)
		if !ok {
			return true
		}
		hasHi = true
	}
	for _, t := range targets {
		if hasLo && comparePrefix(lo, keyPrefix{db: t.db, store: t.store, index: indexBlobEntry}) > 0 {
			continue
		}
		if hasHi && comparePrefix(keyPrefix{db: t.db, store: t.store, index: indexObjectStoreData}, hi) > 0 {
			break
		}
		return true
	}
	return false
}

// Damage returns the file sections skipped while reading LevelDB.
func (db *DB) Damage() []leveldb.Damage {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if len(db.damage) == 0 {
		return []leveldb.Damage{}
	}
	return slices.Clone(db.damage)
}

// BlobFiles lists referenced blob files relative to the blob directory.
func (db *DB) BlobFiles() []string {
	return db.BlobFilesFor(nil)
}

// BlobFilesFor lists referenced blob files relative to the blob directory for
// stores accepted by accept. A nil accept matches all stores. On a lazily
// opened DB, matching stores are loaded on demand.
func (db *DB) BlobFilesFor(accept func(dbNumber, storeID uint64) bool) []string {
	if db.lazyStores {
		if err := db.LoadStores(db.ctx, accept); err != nil && db.ctxErr() == nil {
			if db.logger != nil {
				db.logger.Warn("could not load stores while listing blob files", "dir", db.dir, "error", err)
			}
			db.addDamage([]leveldb.Damage{{
				File:   filepath.Base(db.dir),
				Reason: fmt.Sprintf("load stores for blob listing: %v", err),
			}})
		}
	}
	db.ensureBlobInfo()

	type blobRef struct {
		db, store uint64
		infos     []blobInfo
	}
	db.mu.RLock()
	refs := make([]blobRef, 0, len(db.blobEntries))
	for key, entry := range db.blobEntries {
		refs = append(refs, blobRef{db: key.db, store: key.store, infos: entry.infos})
	}
	db.mu.RUnlock()

	seen := map[string]bool{}
	for _, ref := range refs {
		if accept != nil && !accept(ref.db, ref.store) {
			continue
		}
		for _, info := range ref.infos {
			if info.kind != externalObjectBlob && info.kind != externalObjectFile {
				continue
			}
			seen[blobPath("", ref.db, info.number)] = true
		}
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
