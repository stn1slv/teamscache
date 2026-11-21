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

func TestObjectStoreID(t *testing.T) {
	t.Parallel()
	fixture := filepath.Join("testdata", "fixture")
	db, err := Open(context.Background(), filepath.Join(fixture, "leveldb"), "", slog.New(slog.DiscardHandler))
	require.NoError(t, err)

	id, err := db.ObjectStoreID(1, "wrapped")
	require.NoError(t, err)
	assert.Equal(t, uint64(2), id)

	_, err = db.ObjectStoreID(1, "missing")
	require.ErrorIs(t, err, ErrNotFound)
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

	info, ok := db.blobInfo[blobKey{db: 1, store: 1, key: string([]byte{1, 1, 0, 'k'}), index: 0}]
	require.True(t, ok)
	assert.Equal(t, uint64(9), info.number)
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
	db := &DB{ctx: context.Background(), blobDir: dir, blobInfo: map[blobKey]blobInfo{
		{db: 1, store: 1, key: string(key), index: 0}: {number: 1},
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
	db := &DB{ctx: context.Background(), blobDir: dir, blobInfo: map[blobKey]blobInfo{
		{db: 1, store: 1, key: string(key), index: 0}: {number: 1},
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
