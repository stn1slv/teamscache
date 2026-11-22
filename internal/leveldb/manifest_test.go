package leveldb

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	goleveldb "github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

func TestReadManifestTracksActiveTablesAndLogs(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	db, err := goleveldb.OpenFile(dir, &opt.Options{WriteBuffer: 4 * 1024})
	require.NoError(t, err)
	for i := range 400 {
		require.NoError(t, db.Put(fmt.Appendf(nil, "k-%04d", i), []byte(strings.Repeat("v", 80)), nil))
	}
	require.NoError(t, db.CompactRange(util.Range{}))
	require.NoError(t, db.Close())

	manifest, err := ReadManifest(context.Background(), dir)

	require.NoError(t, err)
	assert.Equal(t, "leveldb.BytewiseComparator", manifest.Comparator)
	assert.NotEmpty(t, manifest.File)
	assert.NotEmpty(t, manifest.Tables)
	assert.Positive(t, manifest.LogNumber)
	assert.Positive(t, manifest.LastSequence)

	for num, info := range manifest.Tables {
		assert.Equal(t, num, info.Number)
		assert.True(t, manifest.IsActiveTable(num))
		assert.NotEmpty(t, info.SmallestUserKey())
		assert.NotEmpty(t, info.LargestUserKey())
		assert.LessOrEqual(t, bytes.Compare(info.SmallestUserKey(), info.LargestUserKey()), 0)
	}
	assert.False(t, manifest.IsActiveTable(999999))
	assert.True(t, manifest.IsActiveLog(manifest.LogNumber))
	assert.True(t, manifest.IsActiveLog(manifest.LogNumber+1))
	assert.False(t, manifest.IsActiveLog(0))
}

func TestReadManifestAppliesDeletionsAndPrevLogNumber(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
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
	addNewFile := func(level, num, size uint64, smallest, largest string) {
		edit = append(edit, editNewFile)
		edit = binary.AppendUvarint(edit, level)
		edit = binary.AppendUvarint(edit, num)
		edit = binary.AppendUvarint(edit, size)
		sk := append([]byte(smallest), 1, 1, 0, 0, 0, 0, 0, 0)
		lk := append([]byte(largest), 1, 2, 0, 0, 0, 0, 0, 0)
		edit = binary.AppendUvarint(edit, uint64(len(sk)))
		edit = append(edit, sk...)
		edit = binary.AppendUvarint(edit, uint64(len(lk)))
		edit = append(edit, lk...)
	}
	addDeletedFile := func(level, num uint64) {
		edit = append(edit, editDeletedFile)
		edit = binary.AppendUvarint(edit, level)
		edit = binary.AppendUvarint(edit, num)
	}

	addStr(editComparator, "idb_cmp1")
	addVarint(editLogNumber, 10)
	addVarint(editPrevLogNumber, 8)
	addVarint(editNextFileNumber, 12)
	addVarint(editLastSequence, 42)
	addNewFile(0, 5, 100, "a", "f")
	addNewFile(1, 6, 200, "g", "z")
	addDeletedFile(0, 5)

	frag := make([]byte, logHeaderSize, logHeaderSize+len(edit))
	binary.LittleEndian.PutUint16(frag[4:], uint16(len(edit)))
	frag[6] = fragFull
	frag = append(frag, edit...)
	binary.LittleEndian.PutUint32(frag, maskedCRC(frag[6:]))

	require.NoError(t, os.WriteFile(filepath.Join(dir, "CURRENT"), []byte("MANIFEST-000001\n"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "MANIFEST-000001"), frag, 0o600))

	manifest, err := ReadManifest(context.Background(), dir)

	require.NoError(t, err)
	assert.Equal(t, "idb_cmp1", manifest.Comparator)
	assert.Equal(t, uint64(10), manifest.LogNumber)
	assert.Equal(t, uint64(8), manifest.PrevLogNumber)
	assert.Equal(t, uint64(12), manifest.NextFileNumber)
	assert.Equal(t, uint64(42), manifest.LastSequence)
	assert.False(t, manifest.IsActiveTable(5), "deleted table is removed")
	assert.True(t, manifest.IsActiveTable(6))
	assert.Equal(t, []byte("g"), manifest.Tables[6].SmallestUserKey())
	assert.Equal(t, []byte("z"), manifest.Tables[6].LargestUserKey())
	assert.True(t, manifest.IsActiveLog(8), "prev log number stays active")
	assert.False(t, manifest.IsActiveLog(9))
	assert.True(t, manifest.IsActiveLog(10))
}

func TestReadManifestErrors(t *testing.T) {
	t.Parallel()
	_, err := ReadManifest(context.Background(), t.TempDir())
	require.ErrorIs(t, err, fs.ErrNotExist)

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "CURRENT"), []byte("../escape\n"), 0o600))
	_, err = ReadManifest(context.Background(), dir)
	require.ErrorIs(t, err, ErrCorrupt)

	dir2 := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir2, "CURRENT"), []byte("MANIFEST-000001\n"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir2, "MANIFEST-000001"), []byte("corrupt"), 0o600))
	_, err = ReadManifest(context.Background(), dir2)
	require.ErrorIs(t, err, ErrCorrupt)
}

func TestReadDirOptionsFiltersFilesBlocksAndRecords(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	db, err := goleveldb.OpenFile(dir, &opt.Options{BlockSize: 128, Compression: opt.SnappyCompression})
	require.NoError(t, err)
	for i := range 200 {
		require.NoError(t, db.Put(fmt.Appendf(nil, "k-%03d", i), []byte(strings.Repeat("x", 40)), nil))
	}
	require.NoError(t, db.CompactRange(util.Range{}))
	require.NoError(t, db.Close())

	// Add an obsolete corrupt table that KeepFile excludes.
	require.NoError(t, os.WriteFile(filepath.Join(dir, "999999.ldb"), []byte("corrupt"), 0o600))

	manifest, err := ReadManifest(context.Background(), dir)
	require.NoError(t, err)

	var blocksSeen, blocksKept int
	wantLo, wantHi := []byte("k-050"), []byte("k-059")
	records, damage, err := ReadDirOptions(context.Background(), dir, slog.New(slog.DiscardHandler), ReadOptions{
		KeepFile: func(number uint64, isLog bool) bool {
			if isLog {
				return manifest.IsActiveLog(number)
			}
			return manifest.IsActiveTable(number)
		},
		KeepBlock: func(prevUserKey, userKey []byte) bool {
			blocksSeen++
			if prevUserKey != nil && bytes.Compare(prevUserKey, wantHi) > 0 {
				return false
			}
			if bytes.Compare(wantLo, userKey) > 0 {
				return false
			}
			blocksKept++
			return true
		},
		KeepRecord: func(userKey []byte) bool {
			return bytes.Compare(userKey, wantLo) >= 0 && bytes.Compare(userKey, wantHi) <= 0
		},
	})

	require.NoError(t, err)
	assert.Empty(t, damage, "obsolete corrupt table was skipped")
	assert.Positive(t, blocksSeen)
	assert.Less(t, blocksKept, blocksSeen, "most blocks were skipped without decompression")
	require.Len(t, records, 10)
	assert.Equal(t, "k-050", string(records[0].UserKey()))
	assert.Equal(t, "k-059", string(records[9].UserKey()))
}
