package indexeddb

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"strconv"
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

	// Corrupt CURRENT falls back to raw scanning and reports manifest damage.
	require.NoError(t, os.WriteFile(filepath.Join(dir, "CURRENT"), []byte("MANIFEST-missing\n"), 0o600))
	fallback, err := OpenLive(context.Background(), dir, "", slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	assert.NotEmpty(t, fallback.Damage())
	require.Len(t, fallback.Databases(), 1)
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
