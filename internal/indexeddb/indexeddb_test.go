package indexeddb

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stn1slv/teamscache/internal/idbtest"
	"github.com/stn1slv/teamscache/internal/leveldb"
	"github.com/stn1slv/teamscache/internal/v8"
)

func normalize(t *testing.T, value any) any {
	t.Helper()
	switch v := value.(type) {
	case nil:
		return nil
	case v8.Undefined:
		return map[string]any{"$undefined": true}
	case bool, string:
		return v
	case int64:
		return map[string]any{"$int": strconv.FormatInt(v, 10)}
	case float64:
		return map[string]any{"$float": strconv.FormatUint(math.Float64bits(v), 10)}
	case *v8.Object:
		pairs := []any{}
		for i, k := range v.Keys() {
			pairs = append(pairs, []any{normalize(t, k), normalize(t, v.Values()[i])})
		}
		return map[string]any{"$dict": pairs}
	case []any:
		items := []any{}
		for _, item := range v {
			items = append(items, normalize(t, item))
		}
		return map[string]any{"$list": items}
	default:
		t.Fatalf("cannot normalize %T", value)
		return nil
	}
}

// TestFixtureMatchesCCL reads the synthetic fixture and compares the result
// with what ccl_chromium_reader read from the same files (golden.json).
func TestFixtureMatchesCCL(t *testing.T) {
	t.Parallel()
	fixture := filepath.Join("testdata", "fixture")
	db, err := Open(context.Background(), filepath.Join(fixture, "leveldb"), filepath.Join(fixture, "blob"), slog.New(slog.DiscardHandler))
	require.NoError(t, err)

	got := map[string]any{"databases": []any{}, "records": []any{}, "bad": 0}
	bad := 0
	var records []any
	databases := make([]any, 0, len(db.Databases()))
	for _, d := range db.Databases() {
		databases = append(databases, map[string]any{"number": d.Number, "origin": d.Origin, "name": d.Name})
		for storeID := uint64(1); storeID <= db.maxStoreID[d.Number]; storeID++ {
			name := db.storeNames[d.Number][storeID]
			for rec := range db.Records(d.Number, storeID, func(error) { bad++ }) {
				if rec.Err != nil {
					continue // golden.json lists only decoded records
				}
				records = append(records, map[string]any{
					"db": d.Number, "store": name, "key": hex.EncodeToString(rec.Key),
					"live": rec.Live, "value": normalize(t, rec.Value),
				})
			}
		}
	}
	got["databases"], got["records"], got["bad"] = databases, records, bad

	golden, err := os.ReadFile(filepath.Join("testdata", "golden.json"))
	require.NoError(t, err)
	gotJSON, err := json.Marshal(got)
	require.NoError(t, err)
	assert.JSONEq(t, string(golden), string(gotJSON))
}

func TestObjectStoreIDs(t *testing.T) {
	t.Parallel()
	fixture := filepath.Join("testdata", "fixture")
	db, err := Open(context.Background(), filepath.Join(fixture, "leveldb"), "", slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	named := func(want string) func(string) bool { return func(n string) bool { return n == want } }

	assert.Equal(t, []uint64{2}, db.ObjectStoreIDs(1, named("wrapped")))
	assert.Empty(t, db.ObjectStoreIDs(1, named("missing")))
}

func TestObjectStoreIDsInAscendingOrder(t *testing.T) {
	t.Parallel()
	db := &DB{
		storeNames: map[uint64]map[uint64]string{1: {4: "b", 1: "a", 3: "a", 2: "c", 9: "d", 0: "z"}},
		maxStoreID: map[uint64]uint64{1: 4},
	}

	got := db.ObjectStoreIDs(1, func(n string) bool { return n != "c" })

	assert.Equal(t, []uint64{1, 4}, got, "the lowest id of a repeated name, and no id below 1 or above the highest store id")
}

func TestBlobValueWithoutBlobFolderIsBad(t *testing.T) {
	t.Parallel()
	fixture := filepath.Join("testdata", "fixture")
	db, err := Open(context.Background(), filepath.Join(fixture, "leveldb"), "", slog.New(slog.DiscardHandler))
	require.NoError(t, err)

	var errs []error
	count := 0
	for rec := range db.Records(1, 2, func(err error) { errs = append(errs, err) }) {
		if rec.Err == nil {
			count++
		}
	}

	assert.Equal(t, 1, count, "only the compressed record can be read")
	require.Len(t, errs, 1)
	require.ErrorIs(t, errs[0], ErrBadValue)
}

func TestMakePrefix(t *testing.T) {
	t.Parallel()
	assert.Equal(t, []byte{0, 0, 0, 0}, makePrefix(0, 0, 0))
	assert.Equal(t, []byte{0, 1, 2, 1}, makePrefix(1, 2, 1))
	assert.Equal(t, []byte{0x24, 0x2c, 0x01, 0x05, 0x01, 0x03}, makePrefix(300, 261, 3))
}

func TestIdbKeyLength(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		key  []byte
		want int
	}{
		{"null", []byte{0, 9}, 1},
		{"string", []byte{1, 2, 0, 'a', 0, 'b', 9}, 6},
		{"number", []byte{3, 1, 2, 3, 4, 5, 6, 7, 8, 9}, 9},
		{"array", []byte{4, 2, 0, 1, 1, 0, 'x'}, 7},
		{"binary", []byte{6, 2, 0xaa, 0xbb, 9}, 4},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := idbKeyLength(tt.key, 0)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
	_, err := idbKeyLength([]byte{5}, 0)
	require.ErrorIs(t, err, ErrBadValue)
}

func TestIdbKeyLengthLimitsNesting(t *testing.T) {
	t.Parallel()
	deep := bytes.Repeat([]byte{4, 1}, maxKeyDepth+10)

	_, err := idbKeyLength(append(deep, 0), 0)

	require.ErrorIs(t, err, ErrBadValue)
}

func TestBlobPath(t *testing.T) {
	t.Parallel()
	assert.Equal(t, filepath.Join("b", "1", "00", "5"), blobPath("b", 1, 5))
	assert.Equal(t, filepath.Join("b", "a", "23", "12345"), blobPath("b", 10, 0x12345), "only the second lowest byte names the folder")
}

func TestReadPrefix(t *testing.T) {
	t.Parallel()
	for _, ids := range [][3]uint64{{0, 0, 0}, {1, 2, 3}, {300, 261, 1}, {1 << 40, 7, 1 << 20}} {
		prefix := makePrefix(ids[0], ids[1], ids[2])
		dbID, storeID, indexID, n, ok := readPrefix(append(prefix, 9, 9))
		require.True(t, ok)
		got := [3]uint64{dbID, storeID, indexID}
		assert.Equal(t, ids, got)
		assert.Len(t, prefix, n)
	}
}

// TestDatabaseListedOnce covers a database name record that is stored in a
// table file and again in a log, as happens before compaction.
func TestDatabaseListedOnce(t *testing.T) {
	t.Parallel()
	key := []byte{0, 0, 0, 0, 0xc9, 1, 0, 'o', 1, 0, 'n'}
	tableKey := append(append([]byte{}, key...), 1, 5, 0, 0, 0, 0, 0, 0) // live, sequence 5
	db := &DB{ctx: context.Background(), records: []leveldb.Record{
		{Key: tableKey, Value: []byte{1}, Seq: 5, State: leveldb.Live, FileType: leveldb.Ldb},
		{Key: key, Value: []byte{1}, Seq: 9, State: leveldb.Live, FileType: leveldb.Log},
	}}

	db.readMetadata()

	assert.Equal(t, []DatabaseID{{Number: 1, Origin: "o", Name: "n"}}, db.Databases())
}

func TestDeletedMetadataStaysDeleted(t *testing.T) {
	t.Parallel()
	key := []byte{0, 0, 0, 0, 0xc9, 1, 0, 'o', 1, 0, 'n'}
	db := &DB{ctx: context.Background(), records: []leveldb.Record{
		{Key: key, Value: []byte{1}, Seq: 5, State: leveldb.Live, FileType: leveldb.Log},
		{Key: key, Seq: 7, State: leveldb.Deleted, FileType: leveldb.Log},
	}}

	db.readMetadata()

	assert.Empty(t, db.Databases(), "the newest version is a deletion")
}

func TestBlobEntryWithHighestSequenceWins(t *testing.T) {
	t.Parallel()
	entry := func(number byte) []byte { return []byte{0, number, 0, 1} }
	key := append(makePrefix(1, 1, indexBlobEntry), 1, 1, 0, 'k')
	db := &DB{ctx: context.Background(), records: []leveldb.Record{
		{Key: key, Value: entry(9), Seq: 20, State: leveldb.Live, FileType: leveldb.Log},
		// A compacted table file has a higher number but holds older data.
		{Key: append(append([]byte{}, key...), 1, 10, 0, 0, 0, 0, 0, 0), Value: entry(3), Seq: 10, State: leveldb.Live, FileType: leveldb.Ldb},
	}}

	db.loadBlobInfo()

	got, ok := db.blobEntries[recordKey{db: 1, store: 1, key: string([]byte{1, 1, 0, 'k'})}]
	require.True(t, ok)
	require.Len(t, got.infos, 1)
	assert.Equal(t, uint64(9), got.infos[0].number)
}

func TestBlobFilesExcludeUnreferencedAndDeletedEntries(t *testing.T) {
	t.Parallel()
	entry := func(number byte) []byte { return []byte{0, number, 0, 1} }
	key := func(name byte) []byte { return append(makePrefix(1, 1, indexBlobEntry), 1, 1, 0, name) }
	db := &DB{ctx: context.Background(), records: []leveldb.Record{
		{Key: key('a'), Value: entry(1), Seq: 1, State: leveldb.Live, FileType: leveldb.Log},
		{Key: key('a'), Value: entry(2), Seq: 2, State: leveldb.Live, FileType: leveldb.Log},
		{Key: key('b'), Value: entry(3), Seq: 3, State: leveldb.Live, FileType: leveldb.Log},
		{Key: key('b'), Seq: 4, State: leveldb.Deleted, FileType: leveldb.Log},
	}}
	assert.Equal(t, []string{filepath.Join("1", "00", "2")}, db.BlobFiles())
}

func TestBlobFilesForFiltersByStore(t *testing.T) {
	t.Parallel()
	entry := func(number byte) []byte { return []byte{0, number, 0, 1} }
	key := func(dbID, storeID uint64, name byte) []byte {
		return append(makePrefix(dbID, storeID, indexBlobEntry), 1, 1, 0, name)
	}
	db := &DB{ctx: context.Background(), records: []leveldb.Record{
		{Key: key(1, 1, 'a'), Value: entry(1), Seq: 1, State: leveldb.Live, FileType: leveldb.Log},
		{Key: key(1, 2, 'b'), Value: entry(2), Seq: 2, State: leveldb.Live, FileType: leveldb.Log},
		{Key: key(2, 1, 'c'), Value: entry(3), Seq: 3, State: leveldb.Live, FileType: leveldb.Log},
	}}

	got := db.BlobFilesFor(func(dbNumber, storeID uint64) bool {
		return dbNumber == 1 && storeID == 2
	})

	assert.Equal(t, []string{filepath.Join("1", "00", "2")}, got)
}

func TestRecordsUsesParsedPrefixLengthForNonMinimalPrefixes(t *testing.T) {
	t.Parallel()
	// Encode dbID=1 with 2 bytes (non-minimal prefix length 6 instead of 4)
	// followed by a string key "k" and Blink-wrapped string "ok".
	key := append([]byte{0x20, 0x01, 0x00, 0x01, 0x01}, 1, 1, 0, 'k')
	val := append([]byte{1}, idbtest.Envelope("ok")...)
	db := &DB{ctx: context.Background(), records: []leveldb.Record{
		{Key: key, Value: val, Seq: 1, State: leveldb.Live, FileType: leveldb.Log},
	}}

	var got []Record
	for rec := range db.Records(1, 1, func(err error) { t.Fatalf("unexpected error: %v", err) }) {
		got = append(got, rec)
	}

	require.Len(t, got, 1)
	assert.Equal(t, "ok", got[0].Value)
}

func TestReadBlobRejectsOversizedFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := blobPath(dir, 1, 1)
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o700))
	file, err := os.Create(path)
	require.NoError(t, err)
	require.NoError(t, file.Truncate(leveldb.MaxDecodedSize+1))
	require.NoError(t, file.Close())
	key := []byte("key")
	db := &DB{ctx: context.Background(), blobDir: dir, blobEntries: map[recordKey]blobEntry{
		{db: 1, store: 1, key: string(key)}: {infos: []blobInfo{{kind: externalObjectBlob, number: 1}}},
	}}

	_, err = db.readBlob(1, 1, key, 0)

	require.ErrorIs(t, err, ErrBadValue)
	assert.Contains(t, err.Error(), "exceeds")
}

func TestRecordsStopWhenCanceled(t *testing.T) {
	t.Parallel()
	fixture := filepath.Join("testdata", "fixture")
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	db, err := Open(ctx, filepath.Join(fixture, "leveldb"), filepath.Join(fixture, "blob"), slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	cancel()
	count, bad := 0, 0
	for range db.Records(1, 1, func(error) { bad++ }) {
		count++
	}
	assert.Zero(t, count)
	assert.Zero(t, bad, "cancellation is not recorded as damaged data")
}

func TestBlobLengthMustMatchItsReference(t *testing.T) {
	t.Parallel()
	data := idbtest.Envelope("value")
	dir := t.TempDir()
	file := blobPath(dir, 1, 1)
	require.NoError(t, os.MkdirAll(filepath.Dir(file), 0o700))
	require.NoError(t, os.WriteFile(file, data, 0o600))
	key := []byte("key")
	db := &DB{ctx: context.Background(), blobDir: dir, blobEntries: map[recordKey]blobEntry{
		{db: 1, store: 1, key: string(key)}: {infos: []blobInfo{{kind: externalObjectBlob, number: 1}}},
	}}
	for _, declared := range []int{len(data) - 1, len(data), len(data) + 1} {
		ref := binary.AppendUvarint([]byte{0xff, blinkWrappedPseudoVer, blinkReplaceWithBlob}, uint64(declared))
		ref = append(ref, 0)
		value, err := db.decodeEnvelope(1, 1, key, ref, 0)
		if declared == len(data) {
			require.NoError(t, err)
			assert.Equal(t, "value", value)
		} else {
			require.ErrorIs(t, err, ErrBadValue)
			assert.Nil(t, value)
			assert.Contains(t, err.Error(), "reference declares")
		}
	}
}

func TestCurrentRecordsSkipsObsoleteBlobVersions(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	oldEnv := idbtest.Envelope("old")
	newEnv := idbtest.Envelope("much longer updated value")
	newFile := blobPath(dir, 1, 2)
	require.NoError(t, os.MkdirAll(filepath.Dir(newFile), 0o700))
	require.NoError(t, os.WriteFile(newFile, newEnv, 0o600))

	blobRef := func(size int) []byte {
		ref := binary.AppendUvarint([]byte{1, 0xff, blinkWrappedPseudoVer, blinkReplaceWithBlob}, uint64(size))
		return append(ref, 0)
	}
	blobMeta := func(number byte, size int) []byte {
		b := []byte{0, number, 0}
		return binary.AppendUvarint(b, uint64(size))
	}
	keyA := []byte{1, 1, 0, 'a'}
	keyB := []byte{1, 1, 0, 'b'}
	recKey := func(k []byte) []byte { return append(makePrefix(1, 1, indexObjectStoreData), k...) }
	metaKey := func(k []byte) []byte { return append(makePrefix(1, 1, indexBlobEntry), k...) }

	db := &DB{ctx: context.Background(), blobDir: dir, records: []leveldb.Record{
		// Key A: older blob-wrapped version (blob 1, smaller size), then newer version (blob 2, larger size).
		{Key: metaKey(keyA), Value: blobMeta(1, len(oldEnv)), Seq: 1, State: leveldb.Live, FileType: leveldb.Log, File: "000001.log", Offset: 10},
		{Key: recKey(keyA), Value: blobRef(len(oldEnv)), Seq: 2, State: leveldb.Live, FileType: leveldb.Log, File: "000001.log", Offset: 20},
		{Key: metaKey(keyA), Value: blobMeta(2, len(newEnv)), Seq: 3, State: leveldb.Live, FileType: leveldb.Log, File: "000001.log", Offset: 30},
		{Key: recKey(keyA), Value: blobRef(len(newEnv)), Seq: 4, State: leveldb.Live, FileType: leveldb.Log, File: "000001.log", Offset: 40},
		// Key B: older blob-wrapped version, later deleted (its blob entry is also deleted).
		{Key: metaKey(keyB), Value: blobMeta(3, len(oldEnv)), Seq: 5, State: leveldb.Live, FileType: leveldb.Log, File: "000001.log", Offset: 50},
		{Key: recKey(keyB), Value: blobRef(len(oldEnv)), Seq: 6, State: leveldb.Live, FileType: leveldb.Log, File: "000001.log", Offset: 60},
		{Key: metaKey(keyB), Seq: 7, State: leveldb.Deleted, FileType: leveldb.Log, File: "000001.log", Offset: 70},
		{Key: recKey(keyB), Seq: 8, State: leveldb.Deleted, FileType: leveldb.Log, File: "000001.log", Offset: 80},
	}}

	var errs []error
	var got []Record
	for rec := range db.CurrentRecords(1, 1, func(err error) { errs = append(errs, err) }) {
		got = append(got, rec)
	}

	assert.Empty(t, errs, "obsolete and deleted blob versions are not decoded")
	require.Len(t, got, 1)
	assert.Equal(t, "much longer updated value", got[0].Value)
	assert.Equal(t, "000001.log", got[0].File)
	assert.Equal(t, int64(40), got[0].Offset)
}

func TestCurrentRecordsParallelDecodingPreservesOrder(t *testing.T) {
	t.Parallel()
	const total = 48
	recs := make([]leveldb.Record, 0, total+3)
	for i := range total {
		k := append(makePrefix(1, 1, indexObjectStoreData), 1, 2, 0, byte('a'+i/26), 0, byte('a'+i%26))
		val := append([]byte{1}, idbtest.Envelope("v-"+strconv.Itoa(i))...)
		recs = append(recs, leveldb.Record{
			Key: k, Value: val, Seq: uint64(i + 1), State: leveldb.Live,
			FileType: leveldb.Log, File: "/tmp/000002.log", Offset: int64(100 + i),
		})
	}
	// Overwrite record 5 with a corrupt version; it should stay at position 5 with Err set.
	corruptKey := recs[5].Key
	recs = append(recs, leveldb.Record{
		Key: corruptKey, Value: []byte{1, 0x00}, Seq: 1000, State: leveldb.Live,
		FileType: leveldb.Log, File: "/tmp/000002.log", Offset: 999,
	})
	// Delete record 10; it should disappear without errors.
	recs = append(recs, leveldb.Record{
		Key: recs[10].Key, Seq: 1001, State: leveldb.Deleted,
		FileType: leveldb.Log, File: "/tmp/000002.log", Offset: 1000,
	})

	db := &DB{ctx: context.Background(), records: recs}
	var bad []error
	var got []Record
	for rec := range db.CurrentRecords(1, 1, func(err error) { bad = append(bad, err) }) {
		got = append(got, rec)
	}

	require.Len(t, bad, 1)
	assert.Contains(t, bad[0].Error(), "000002.log")
	assert.Contains(t, bad[0].Error(), "offset 999")
	require.Len(t, got, total-1)
	assert.Equal(t, "v-0", got[0].Value)
	require.Error(t, got[5].Err)
	assert.Equal(t, "000002.log", got[5].File)
	assert.Equal(t, int64(999), got[5].Offset)
	assert.Equal(t, "v-11", got[10].Value, "record 10 was deleted and remaining order is preserved")
}

func TestParseBlobEntriesHandlesFileSystemHandleAndErrors(t *testing.T) {
	t.Parallel()
	// Entry 0: NativeFileSystemHandle (kind 2) with 3-byte token.
	// Entry 1: Blob (kind 0) number 7, empty mime, size 10.
	raw := []byte{2, 3, 't', 'o', 'k', 0, 7, 0, 10}
	infos, err := parseBlobEntries(raw)
	require.NoError(t, err)
	require.Len(t, infos, 2)
	assert.Equal(t, byte(externalObjectFileSystemHandle), infos[0].kind)
	assert.Equal(t, byte(externalObjectBlob), infos[1].kind)
	assert.Equal(t, uint64(7), infos[1].number)

	// Truncated second entry preserves the first entry and reports an error.
	trunc, err := parseBlobEntries([]byte{0, 5, 0, 10, 0, 9})
	require.ErrorIs(t, err, ErrBadValue)
	require.Len(t, trunc, 1)
	assert.Equal(t, uint64(5), trunc[0].number)

	// Unknown external object type returns an error.
	_, err = parseBlobEntries([]byte{9})
	require.ErrorIs(t, err, ErrBadValue)

	// In DB.readBlob, index 0 (file handle) fails cleanly, while index 1 resolves blob 7.
	dir := t.TempDir()
	p := blobPath(dir, 1, 7)
	require.NoError(t, os.MkdirAll(filepath.Dir(p), 0o700))
	require.NoError(t, os.WriteFile(p, []byte("blob-data"), 0o600))
	key := []byte{1, 1, 0, 'k'}
	db := &DB{ctx: context.Background(), blobDir: dir, records: []leveldb.Record{
		{Key: append(makePrefix(1, 1, indexBlobEntry), key...), Value: raw, Seq: 1, State: leveldb.Live, FileType: leveldb.Log},
	}}

	_, err = db.readBlob(1, 1, key, 0)
	require.ErrorIs(t, err, ErrBadValue)
	assert.Contains(t, err.Error(), "unsupported type 2")

	got, err := db.readBlob(1, 1, key, 1)
	require.NoError(t, err)
	assert.Equal(t, []byte("blob-data"), got)
}

func TestBlinkTrailerBounds(t *testing.T) {
	t.Parallel()
	db := &DB{ctx: context.Background()}
	payload := idbtest.Serialize("hello")

	makeBuf := func(offset uint64, length uint32, tail []byte) []byte {
		buf := []byte{0xff, 21, 0xfe}
		buf = binary.BigEndian.AppendUint64(buf, offset)
		buf = binary.BigEndian.AppendUint32(buf, length)
		buf = append(buf, payload...)
		return append(buf, tail...)
	}

	// Zero offset and length: valid (no trailer).
	v, err := db.decodeEnvelope(1, 1, nil, makeBuf(0, 0, nil), 0)
	require.NoError(t, err)
	assert.Equal(t, "hello", v)

	// Valid non-zero trailer at the end of the buffer.
	trailerOffset := uint64(2 + blinkTrailerSize + len(payload))
	v, err = db.decodeEnvelope(1, 1, nil, makeBuf(trailerOffset, 4, []byte{0xa0, 1, 2, 3}), 0)
	require.NoError(t, err)
	assert.Equal(t, "hello", v)

	// Invalid bounds: offset before payload, offset past EOF, length past EOF, zero offset with non-zero length.
	for _, bad := range [][]byte{
		makeBuf(0, 4, nil),
		makeBuf(5, 2, nil),
		makeBuf(trailerOffset+100, 1, nil),
		makeBuf(trailerOffset, 10, []byte{1, 2}),
	} {
		_, err := db.decodeEnvelope(1, 1, nil, bad, 0)
		require.ErrorIs(t, err, ErrBadValue)
		assert.Contains(t, err.Error(), "trailer out of bounds")
	}
}

func TestKeyErrorsIncludeFileAndOffset(t *testing.T) {
	t.Parallel()
	db := &DB{ctx: context.Background(), records: []leveldb.Record{
		{Key: append(makePrefix(1, 1, indexObjectStoreData), 99), Seq: 1, State: leveldb.Live, FileType: leveldb.Log, File: "/dir/000005.log", Offset: 77},
	}}
	var errs []error
	for range db.Records(1, 1, func(err error) { errs = append(errs, err) }) {
	}
	require.Len(t, errs, 1)
	assert.Contains(t, errs[0].Error(), "000005.log")
	assert.Contains(t, errs[0].Error(), "offset 77")
}

func TestOpenLiveAndLoadStoresSkipObsoleteAndUnrelatedData(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "live.leveldb")
	p := func(k string, v any) idbtest.Prop { return idbtest.Prop{Key: k, Value: v} }
	require.NoError(t, idbtest.Write(dir, "", []idbtest.Database{
		{Number: 1, Name: "db1", Stores: []idbtest.Store{
			{Name: "wanted", Records: []idbtest.Record{{Key: "k1", Value: idbtest.Obj{p("v", "ok")}}}},
			{Name: "ignored", Records: []idbtest.Record{{Key: "k2", Value: idbtest.Obj{p("v", "skip")}}}},
		}},
	}))
	// Add an obsolete corrupt table not referenced by CURRENT/MANIFEST.
	require.NoError(t, os.WriteFile(filepath.Join(dir, "999999.ldb"), []byte("corrupt"), 0o600))

	db, err := OpenLive(context.Background(), dir, "", slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	assert.Empty(t, db.Damage(), "obsolete table not in manifest is ignored")
	require.Len(t, db.Databases(), 1)
	require.NoError(t, db.LoadStores(context.Background(), func(dbNumber, storeID uint64) bool {
		return dbNumber == 1 && storeID == 1
	}))
	var got []Record
	for rec := range db.CurrentRecords(1, 1, func(err error) { t.Fatalf("unexpected error: %v", err) }) {
		got = append(got, rec)
	}
	require.Len(t, got, 1)

	// Corrupt CURRENT (pointing to a missing manifest) in a clean directory
	// falls back to raw scanning and reports damage specifically for CURRENT.
	require.NoError(t, os.Remove(filepath.Join(dir, "999999.ldb")))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "CURRENT"), []byte("MANIFEST-000099\n"), 0o600))
	fallback, err := OpenLive(context.Background(), dir, "", slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	require.Len(t, fallback.Damage(), 1)
	assert.Equal(t, "CURRENT", fallback.Damage()[0].File)
	require.Len(t, fallback.Databases(), 1)
}

func testMaskedCRC(data []byte) uint32 {
	crc := crc32.Checksum(data, crc32.MakeTable(crc32.Castagnoli))
	return (crc>>15 | crc<<17) + 0xa282ead8
}

func testMakeBlock(entries [][2][]byte) []byte {
	var block []byte
	for _, kv := range entries {
		block = binary.AppendUvarint(block, 0)
		block = binary.AppendUvarint(block, uint64(len(kv[0])))
		block = binary.AppendUvarint(block, uint64(len(kv[1])))
		block = append(block, kv[0]...)
		block = append(block, kv[1]...)
	}
	block = binary.LittleEndian.AppendUint32(block, 0) // first restart at 0
	block = binary.LittleEndian.AppendUint32(block, 1) // 1 restart
	return block
}

func testAppendBlock(dst, payload []byte, corruptCRC bool) ([]byte, uint64, uint64) {
	offset := uint64(len(dst))
	length := uint64(len(payload))
	dst = append(dst, payload...)
	dst = append(dst, 0) // uncompressed
	crc := testMaskedCRC(dst[offset:])
	if corruptCRC {
		crc ^= 1
	}
	dst = binary.LittleEndian.AppendUint32(dst, crc)
	return dst, offset, length
}

func testBuildSSTable(blocks []struct {
	entries [][2][]byte
	corrupt bool
},
) []byte {
	table := make([]byte, 0, 256)
	idxEntries := make([][2][]byte, 0, len(blocks))
	for _, b := range blocks {
		raw := testMakeBlock(b.entries)
		var off, length uint64
		table, off, length = testAppendBlock(table, raw, b.corrupt)
		var handle []byte
		handle = binary.AppendUvarint(handle, off)
		handle = binary.AppendUvarint(handle, length)
		lastKey := b.entries[len(b.entries)-1][0]
		idxEntries = append(idxEntries, [2][]byte{lastKey, handle})
	}
	var metaOff, metaLen, idxOff, idxLen uint64
	table, metaOff, metaLen = testAppendBlock(table, testMakeBlock(nil), false)
	table, idxOff, idxLen = testAppendBlock(table, testMakeBlock(idxEntries), false)
	var footer [48]byte
	var handles []byte
	handles = binary.AppendUvarint(handles, metaOff)
	handles = binary.AppendUvarint(handles, metaLen)
	handles = binary.AppendUvarint(handles, idxOff)
	handles = binary.AppendUvarint(handles, idxLen)
	copy(footer[:], handles)
	binary.LittleEndian.PutUint64(footer[40:], 0xdb4775248b80fb57)
	return append(table, footer[:]...)
}

func testInternalKey(userKey []byte, seq uint64, live bool) []byte {
	kind := uint64(0)
	if live {
		kind = 1
	}
	out := append([]byte(nil), userKey...)
	return binary.LittleEndian.AppendUint64(out, (seq<<8)|kind)
}

func testWriteIDBManifest(t *testing.T, dir string, tables []struct {
	num               uint64
	size              uint64
	smallest, largest []byte
},
) {
	t.Helper()
	var edit []byte
	addStr := func(tag byte, s string) {
		edit = append(edit, tag)
		edit = binary.AppendUvarint(edit, uint64(len(s)))
		edit = append(edit, s...)
	}
	addVarint := func(tag byte, v uint64) {
		edit = append(edit, tag)
		edit = binary.AppendUvarint(edit, v)
	}
	addStr(1, idbComparatorName)
	addVarint(2, 10)
	addVarint(3, 12)
	addVarint(4, 100)
	for _, tbl := range tables {
		edit = append(edit, 7) // NewFile
		edit = binary.AppendUvarint(edit, 0)
		edit = binary.AppendUvarint(edit, tbl.num)
		edit = binary.AppendUvarint(edit, tbl.size)
		edit = binary.AppendUvarint(edit, uint64(len(tbl.smallest)))
		edit = append(edit, tbl.smallest...)
		edit = binary.AppendUvarint(edit, uint64(len(tbl.largest)))
		edit = append(edit, tbl.largest...)
	}
	frag := make([]byte, 7, 7+len(edit))
	binary.LittleEndian.PutUint16(frag[4:], uint16(len(edit)))
	frag[6] = 1 // full
	frag = append(frag, edit...)
	binary.LittleEndian.PutUint32(frag, testMaskedCRC(frag[6:]))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "CURRENT"), []byte("MANIFEST-000001\n"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "MANIFEST-000001"), frag, 0o600))
}

func testIDBString(s string) []byte {
	out := binary.AppendUvarint(nil, uint64(len(s)))
	for i := range len(s) {
		out = append(out, 0, s[i])
	}
	return out
}

func testUTF16BE(s string) []byte {
	out := make([]byte, 0, len(s)*2)
	for i := range len(s) {
		out = append(out, 0, s[i])
	}
	return out
}

func TestOpenLiveIDBCmp1LazyPruningAndOnDemandLoad(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	dir, blobDir := filepath.Join(root, "idb.leveldb"), filepath.Join(root, "idb.blob")
	require.NoError(t, os.MkdirAll(dir, 0o700))

	// Write a blob file for key "k2" in db 1.
	blobVal := idbtest.Envelope("from-blob")
	bp := blobPath(blobDir, 1, 1)
	require.NoError(t, os.MkdirAll(filepath.Dir(bp), 0o700))
	require.NoError(t, os.WriteFile(bp, blobVal, 0o600))

	// Metadata keys in idb_cmp1 order.
	dbNameKey := append([]byte{0, 0, 0, 0, databaseNamePrefixByte}, testIDBString("https_example.test_0@1")...)
	dbNameKey = append(dbNameKey, testIDBString("db1")...)
	maxStoreKey := append(makePrefix(1, 0, 0), dbMetaMaxObjectStoreID)
	store1NameKey := append(makePrefix(1, 0, 0), objectStoreMetaByte, 1, objectStoreMetaName)
	store2NameKey := append(makePrefix(1, 0, 0), objectStoreMetaByte, 2, objectStoreMetaName)

	// Store 1 keys: k1 (inline), k2 (blob), k3 (tombstone), plus blob entry and corrupt secondary index block.
	k1 := append([]byte{1}, testIDBString("k1")...)
	k2 := append([]byte{1}, testIDBString("k2")...)
	k3 := append([]byte{1}, testIDBString("k3")...)
	blobRef := append(binary.AppendUvarint([]byte{1, 0xff, blinkWrappedPseudoVer, blinkReplaceWithBlob}, uint64(len(blobVal))), 0)
	blobMeta := binary.AppendUvarint([]byte{0, 1, 0}, uint64(len(blobVal)))
	secIdxKeyA := append(makePrefix(1, 1, 30), k1...)
	secIdxKeyB := append(makePrefix(1, 1, 30), k2...)

	// Store 2 key.
	kOther := append([]byte{1}, testIDBString("other")...)

	tableBytes := testBuildSSTable([]struct {
		entries [][2][]byte
		corrupt bool
	}{
		{entries: [][2][]byte{
			{testInternalKey(dbNameKey, 1, true), []byte{1}},
			{testInternalKey(maxStoreKey, 2, true), []byte{2}},
			{testInternalKey(store1NameKey, 3, true), testUTF16BE("wanted")},
			{testInternalKey(store2NameKey, 4, true), testUTF16BE("other")},
		}},
		{entries: [][2][]byte{
			{testInternalKey(append(makePrefix(1, 1, 1), k1...), 10, true), append([]byte{1}, idbtest.Envelope("inline-v")...)},
			{testInternalKey(append(makePrefix(1, 1, 1), k2...), 11, true), blobRef},
			{testInternalKey(append(makePrefix(1, 1, 1), k3...), 12, true), append([]byte{1}, idbtest.Envelope("dead")...)},
			{testInternalKey(append(makePrefix(1, 1, 1), k3...), 13, false), nil},
			{testInternalKey(append(makePrefix(1, 1, 3), k2...), 14, true), blobMeta},
		}},
		// Boundary secondary-index block immediately after (1,1,3): conservatively
		// read on (1,1) boundary, then records dropped by KeepRecord.
		{entries: [][2][]byte{
			{testInternalKey(secIdxKeyA, 15, true), []byte("ignored-boundary")},
		}},
		// Interior secondary-index block strictly past (1,1,3) and before (1,2,1):
		// must be skipped by idb_cmp1 block pruning for both store 1 and store 2.
		{entries: [][2][]byte{
			{testInternalKey(secIdxKeyB, 16, true), []byte("ignored-corrupt")},
		}, corrupt: true},
		{entries: [][2][]byte{
			{testInternalKey(append(makePrefix(1, 2, 1), kOther...), 20, true), append([]byte{1}, idbtest.Envelope("store2-v")...)},
		}},
	})
	require.NoError(t, os.WriteFile(filepath.Join(dir, "000001.ldb"), tableBytes, 0o600))
	testWriteIDBManifest(t, dir, []struct {
		num               uint64
		size              uint64
		smallest, largest []byte
	}{{
		num: 1, size: uint64(len(tableBytes)),
		smallest: testInternalKey(dbNameKey, 1, true),
		largest:  testInternalKey(append(makePrefix(1, 2, 1), kOther...), 20, true),
	}})

	db, err := OpenLive(context.Background(), dir, blobDir, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	assert.True(t, db.lazyStores, "idb_cmp1 manifest enables lazy mode")
	assert.Empty(t, db.loadedStores, "stores are not loaded until requested")
	assert.Empty(t, db.Damage(), "corrupt secondary-index block was pruned")
	require.Len(t, db.Databases(), 1)

	// Calling BlobFilesFor on a freshly opened lazy DB loads matching stores on demand.
	gotBlobs := db.BlobFilesFor(func(dbNumber, storeID uint64) bool {
		return dbNumber == 1 && storeID == 1
	})
	assert.Equal(t, []string{blobPath("", 1, 1)}, gotBlobs)
	assert.True(t, db.loadedStores[storeAddr{db: 1, store: 1}])
	assert.False(t, db.loadedStores[storeAddr{db: 1, store: 2}], "ignored store stays unloaded")
	assert.Empty(t, db.Damage(), "secondary index block stayed pruned during store load")

	var got1 []any
	for rec := range db.CurrentRecords(1, 1, func(err error) { t.Fatalf("unexpected error: %v", err) }) {
		got1 = append(got1, rec.Value)
	}
	assert.Equal(t, []any{"inline-v", "from-blob"}, got1, "tombstone k3 omitted, blob k2 resolved")

	// Reading store 2 without calling LoadStores first auto-loads it on demand.
	var got2 []any
	for rec := range db.CurrentRecords(1, 2, func(err error) { t.Fatalf("unexpected error: %v", err) }) {
		got2 = append(got2, rec.Value)
	}
	assert.True(t, db.loadedStores[storeAddr{db: 1, store: 2}])
	assert.Equal(t, []any{"store2-v"}, got2)
	assert.Empty(t, db.Damage())
}

func TestOpenLiveChecksPrevLogNumberAndTableSize(t *testing.T) {
	t.Parallel()
	dbNameKey := append([]byte{0, 0, 0, 0, databaseNamePrefixByte}, testIDBString("https_example.test_0@1")...)
	dbNameKey = append(dbNameKey, testIDBString("db1")...)
	tableBytes := testBuildSSTable([]struct {
		entries [][2][]byte
		corrupt bool
	}{{entries: [][2][]byte{{testInternalKey(dbNameKey, 1, true), []byte{1}}}}})

	// Truncated table (manifest declares larger size) falls back and reports damage.
	dirSize := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dirSize, "000001.ldb"), tableBytes, 0o600))
	testWriteIDBManifest(t, dirSize, []struct {
		num               uint64
		size              uint64
		smallest, largest []byte
	}{{
		num: 1, size: uint64(len(tableBytes) + 64),
		smallest: testInternalKey(dbNameKey, 1, true),
		largest:  testInternalKey(dbNameKey, 1, true),
	}})
	dbSize, err := OpenLive(context.Background(), dirSize, "", slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	assert.False(t, dbSize.lazyStores)
	require.Len(t, dbSize.Damage(), 1)
	assert.Equal(t, "000001.ldb", dbSize.Damage()[0].File)
	assert.Contains(t, dbSize.Damage()[0].Reason, "does not match manifest size")

	// Missing nonzero PrevLogNumber triggers fallback and reports the missing log.
	dirPrevLog := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dirPrevLog, "000001.ldb"), tableBytes, 0o600))
	var edit []byte
	edit = append(edit, 1) // Comparator
	edit = binary.AppendUvarint(edit, uint64(len(idbComparatorName)))
	edit = append(edit, idbComparatorName...)
	edit = append(edit, 2, 10) // LogNumber
	edit = append(edit, 9, 8)  // PrevLogNumber = 8 (missing on disk)
	edit = append(edit, 3, 12) // NextFileNumber
	edit = append(edit, 4, 1)  // LastSequence
	edit = append(edit, 7, 0, 1)
	edit = binary.AppendUvarint(edit, uint64(len(tableBytes)))
	ik := testInternalKey(dbNameKey, 1, true)
	edit = binary.AppendUvarint(edit, uint64(len(ik)))
	edit = append(edit, ik...)
	edit = binary.AppendUvarint(edit, uint64(len(ik)))
	edit = append(edit, ik...)
	frag := make([]byte, 7, 7+len(edit))
	binary.LittleEndian.PutUint16(frag[4:], uint16(len(edit)))
	frag[6] = 1
	frag = append(frag, edit...)
	binary.LittleEndian.PutUint32(frag, testMaskedCRC(frag[6:]))
	require.NoError(t, os.WriteFile(filepath.Join(dirPrevLog, "CURRENT"), []byte("MANIFEST-000001\n"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dirPrevLog, "MANIFEST-000001"), frag, 0o600))

	dbPrev, err := OpenLive(context.Background(), dirPrevLog, "", slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	assert.False(t, dbPrev.lazyStores)
	require.Len(t, dbPrev.Damage(), 1)
	assert.Equal(t, "000008.log", dbPrev.Damage()[0].File)
	assert.Contains(t, dbPrev.Damage()[0].Reason, "previous log")
}

func TestLoadStoresDetectsDisappearedTableAndContextCancellation(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbNameKey := append([]byte{0, 0, 0, 0, databaseNamePrefixByte}, testIDBString("https_example.test_0@1")...)
	dbNameKey = append(dbNameKey, testIDBString("db1")...)
	maxStoreKey := append(makePrefix(1, 0, 0), dbMetaMaxObjectStoreID)
	store1NameKey := append(makePrefix(1, 0, 0), objectStoreMetaByte, 1, objectStoreMetaName)
	k1 := append([]byte{1}, testIDBString("k1")...)

	metaTable := testBuildSSTable([]struct {
		entries [][2][]byte
		corrupt bool
	}{{entries: [][2][]byte{
		{testInternalKey(dbNameKey, 1, true), []byte{1}},
		{testInternalKey(maxStoreKey, 2, true), []byte{1}},
		{testInternalKey(store1NameKey, 3, true), testUTF16BE("s1")},
	}}})
	dataTable := testBuildSSTable([]struct {
		entries [][2][]byte
		corrupt bool
	}{{entries: [][2][]byte{
		{testInternalKey(append(makePrefix(1, 1, 1), k1...), 4, true), append([]byte{1}, idbtest.Envelope("v1")...)},
	}}})
	require.NoError(t, os.WriteFile(filepath.Join(dir, "000001.ldb"), metaTable, 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "000002.ldb"), dataTable, 0o600))
	testWriteIDBManifest(t, dir, []struct {
		num               uint64
		size              uint64
		smallest, largest []byte
	}{
		{num: 1, size: uint64(len(metaTable)), smallest: testInternalKey(dbNameKey, 1, true), largest: testInternalKey(store1NameKey, 3, true)},
		{num: 2, size: uint64(len(dataTable)), smallest: testInternalKey(append(makePrefix(1, 1, 1), k1...), 4, true), largest: testInternalKey(append(makePrefix(1, 1, 1), k1...), 4, true)},
	})

	// Cancel OpenLive ctx before calling LoadStores with Background ctx.
	openCtx, cancel := context.WithCancel(context.Background())
	dbCanceled, err := OpenLive(openCtx, dir, "", slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	cancel()
	require.ErrorIs(t, dbCanceled.LoadStores(context.Background(), nil), context.Canceled)
	assert.Empty(t, dbCanceled.loadedStores)

	// Remove data table after OpenLive; lazy read reports damage and skips marking loaded.
	db, err := OpenLive(context.Background(), dir, "", slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	require.NoError(t, os.Remove(filepath.Join(dir, "000002.ldb")))

	var onBadErrs []error
	for range db.CurrentRecords(1, 1, func(err error) { onBadErrs = append(onBadErrs, err) }) {
		t.Fatal("expected no records when table disappeared")
	}
	assert.NotEmpty(t, onBadErrs)
	assert.False(t, db.loadedStores[storeAddr{db: 1, store: 1}])
	damage := db.Damage()
	files := make([]string, 0, len(damage))
	for _, d := range damage {
		files = append(files, d.File)
	}
	assert.Contains(t, files, "000002.ldb")
}

func TestCurrentRecordsParallelBlobsNilOnBadAndConcurrentAccess(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	dir, blobDir := filepath.Join(root, "cache.leveldb"), filepath.Join(root, "cache.blob")
	const count = parallelDecodeThreshold + 12
	recs1 := make([]idbtest.Record, 0, count)
	recs2 := make([]idbtest.Record, 0, count)
	for i := range count {
		recs1 = append(recs1, idbtest.Record{Key: fmt.Sprintf("k-%03d", i), Value: fmt.Sprintf("blob-%d", i), Wrapping: idbtest.Blob})
		recs2 = append(recs2, idbtest.Record{Key: fmt.Sprintf("m-%03d", i), Value: fmt.Sprintf("inline-%d", i)})
	}
	require.NoError(t, idbtest.Write(dir, blobDir, []idbtest.Database{{
		Number: 1, Name: "db1",
		Stores: []idbtest.Store{
			{Name: "s1", Records: recs1},
			{Name: "s2", Records: recs2},
		},
	}}))

	db, err := OpenLive(context.Background(), dir, blobDir, slog.New(slog.DiscardHandler))
	require.NoError(t, err)

	// Verify concurrent CurrentRecords, LoadStores, BlobFiles, and Damage pass -race
	// and nil onBad callbacks do not panic.
	var wg sync.WaitGroup
	wg.Add(3)
	var got1, got2 []any
	go func() {
		defer wg.Done()
		for rec := range db.CurrentRecords(1, 1, nil) {
			if rec.Err == nil {
				got1 = append(got1, rec.Value)
			}
		}
	}()
	go func() {
		defer wg.Done()
		for rec := range db.CurrentRecords(1, 2, nil) {
			if rec.Err == nil {
				got2 = append(got2, rec.Value)
			}
		}
	}()
	go func() {
		defer wg.Done()
		_ = db.BlobFiles()
		_ = db.Damage()
	}()
	wg.Wait()

	assert.Len(t, got1, count)
	assert.Len(t, got2, count)
	assert.Equal(t, "blob-0", got1[0])
	assert.Equal(t, fmt.Sprintf("blob-%d", count-1), got1[count-1])
}

func TestOpenLiveMissingManifestTableFallsBackAndReportsDamage(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbNameKey := append([]byte{0, 0, 0, 0, databaseNamePrefixByte}, testIDBString("https_example.test_0@1")...)
	dbNameKey = append(dbNameKey, testIDBString("db1")...)
	tableBytes := testBuildSSTable([]struct {
		entries [][2][]byte
		corrupt bool
	}{{entries: [][2][]byte{{testInternalKey(dbNameKey, 1, true), []byte{1}}}}})
	require.NoError(t, os.WriteFile(filepath.Join(dir, "000001.ldb"), tableBytes, 0o600))
	testWriteIDBManifest(t, dir, []struct {
		num               uint64
		size              uint64
		smallest, largest []byte
	}{
		{num: 1, size: uint64(len(tableBytes)), smallest: testInternalKey(dbNameKey, 1, true), largest: testInternalKey(dbNameKey, 1, true)},
		{num: 5, size: 100, smallest: testInternalKey(makePrefix(2, 1, 1), 2, true), largest: testInternalKey(makePrefix(2, 1, 1), 2, true)},
	})

	db, err := OpenLive(context.Background(), dir, "", slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	assert.False(t, db.lazyStores, "missing manifest table triggers raw fallback")
	require.Len(t, db.Databases(), 1)
	require.Len(t, db.Damage(), 1)
	assert.Equal(t, "000005.ldb", db.Damage()[0].File)
	assert.Contains(t, db.Damage()[0].Reason, "missing table")
}

func TestCurrentRecordsEarlyStopWithBoundedWindows(t *testing.T) {
	t.Parallel()
	const total = parallelDecodeWindowSize*2 + 50
	recs := make([]leveldb.Record, 0, total)
	for i := range total {
		k := append(makePrefix(1, 1, indexObjectStoreData), 1, 3, 0, byte('a'+i/676), 0, byte('a'+(i/26)%26), 0, byte('a'+i%26))
		val := append([]byte{1}, idbtest.Envelope("v-"+strconv.Itoa(i))...)
		recs = append(recs, leveldb.Record{Key: k, Value: val, Seq: uint64(i + 1), State: leveldb.Live, FileType: leveldb.Log, File: "000001.log"})
	}
	db := &DB{ctx: context.Background(), records: recs}
	var got []any
	for rec := range db.CurrentRecords(1, 1, func(err error) { t.Fatalf("unexpected error: %v", err) }) {
		got = append(got, rec.Value)
		if len(got) == 3 {
			break
		}
	}
	assert.Equal(t, []any{"v-0", "v-1", "v-2"}, got)
}

func TestPrefixRangeOverlapHelpers(t *testing.T) {
	t.Parallel()
	assert.True(t, overlapsMetadata(nil, makePrefix(1, 1, 1)))
	assert.True(t, overlapsMetadata(makePrefix(0, 0, 0), makePrefix(1, 1, 1)))
	assert.True(t, overlapsMetadata(makePrefix(1, 0, 0), makePrefix(1, 2, 1)))
	assert.True(t, overlapsMetadata(makePrefix(1, 2, 1), makePrefix(2, 1, 1)), "spans into db 2 metadata at (2,0,0)")
	assert.False(t, overlapsMetadata(makePrefix(1, 1, 1), makePrefix(1, 3, 30)), "all keys stay inside db 1 store >= 1")

	targets := []storeAddr{{db: 2, store: 1}, {db: 4, store: 2}}
	assert.False(t, overlapsStores(makePrefix(1, 1, 1), makePrefix(1, 9, 30), targets))
	assert.False(t, overlapsStores(makePrefix(2, 1, 30), makePrefix(2, 1, 40), targets), "secondary indexes are skipped")
	assert.True(t, overlapsStores(makePrefix(2, 1, 1), makePrefix(2, 1, 1), targets))
	assert.True(t, overlapsStores(makePrefix(2, 1, 1), makePrefix(2, 1, 30), targets))
	assert.True(t, overlapsStores(makePrefix(2, 1, 3), makePrefix(2, 2, 1), targets))
	assert.True(t, overlapsStores(makePrefix(3, 1, 1), makePrefix(5, 1, 1), targets))
	assert.False(t, overlapsStores(makePrefix(4, 3, 1), makePrefix(9, 1, 1), targets))
}
