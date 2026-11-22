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
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	goleveldb "github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

func wrapManifestEdit(edit []byte) []byte {
	frag := make([]byte, logHeaderSize, logHeaderSize+len(edit))
	binary.LittleEndian.PutUint16(frag[4:], uint16(len(edit)))
	frag[6] = fragFull
	frag = append(frag, edit...)
	binary.LittleEndian.PutUint32(frag, maskedCRC(frag[6:]))
	return frag
}

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

func TestReadManifestAppliesDeletionsTrivialMoveAndPrevLogNumber(t *testing.T) {
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
	// Trivial move where NewFile at level 1 appears before DeletedFile at level 0.
	addNewFile(0, 7, 150, "m", "n")
	addNewFile(1, 7, 150, "m", "n")
	addDeletedFile(0, 7)

	require.NoError(t, os.WriteFile(filepath.Join(dir, "CURRENT"), []byte("MANIFEST-000001\n"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "MANIFEST-000001"), wrapManifestEdit(edit), 0o600))

	manifest, err := ReadManifest(context.Background(), dir)

	require.NoError(t, err)
	assert.Equal(t, "idb_cmp1", manifest.Comparator)
	assert.Equal(t, uint64(10), manifest.LogNumber)
	assert.Equal(t, uint64(8), manifest.PrevLogNumber)
	assert.Equal(t, uint64(12), manifest.NextFileNumber)
	assert.Equal(t, uint64(42), manifest.LastSequence)
	assert.False(t, manifest.IsActiveTable(5), "deleted table is removed")
	assert.True(t, manifest.IsActiveTable(6))
	assert.True(t, manifest.IsActiveTable(7), "trivial-move table stays live at its new level")
	assert.Equal(t, uint64(1), manifest.Tables[7].Level)
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

	// Path traversal and non-MANIFEST sibling names are rejected.
	for _, target := range []string{"../escape\n", "LOCK\n", "000001.ldb\n"} {
		dir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(dir, "CURRENT"), []byte(target), 0o600))
		_, err = ReadManifest(context.Background(), dir)
		require.ErrorIs(t, err, ErrCorrupt, target)
		require.NotErrorIs(t, err, fs.ErrNotExist, target)
	}

	// Missing manifest target referenced by CURRENT returns ErrCorrupt, not fs.ErrNotExist.
	dirMissing := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dirMissing, "CURRENT"), []byte("MANIFEST-000099\n"), 0o600))
	_, err = ReadManifest(context.Background(), dirMissing)
	require.ErrorIs(t, err, ErrCorrupt)
	require.NotErrorIs(t, err, fs.ErrNotExist)

	// Corrupt manifest bytes return ErrCorrupt.
	dirCorrupt := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dirCorrupt, "CURRENT"), []byte("MANIFEST-000001\n"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dirCorrupt, "MANIFEST-000001"), []byte("corrupt"), 0o600))
	_, err = ReadManifest(context.Background(), dirCorrupt)
	require.ErrorIs(t, err, ErrCorrupt)

	// Incomplete manifest missing required fields (e.g. only Comparator) returns ErrCorrupt.
	dirIncomplete := t.TempDir()
	edit := append([]byte{editComparator, 8}, "idb_cmp1"...)
	require.NoError(t, os.WriteFile(filepath.Join(dirIncomplete, "CURRENT"), []byte("MANIFEST-000001\n"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dirIncomplete, "MANIFEST-000001"), wrapManifestEdit(edit), 0o600))
	_, err = ReadManifest(context.Background(), dirIncomplete)
	require.ErrorIs(t, err, ErrCorrupt)
	assert.Contains(t, err.Error(), "missing required metadata")
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

	var blocksSeen, blocksKept atomic.Int64
	wantLo, wantHi := []byte("k-050"), []byte("k-059")
	records, damage, err := ReadDirOptions(context.Background(), dir, slog.New(slog.DiscardHandler), ReadOptions{
		KeepFile: func(number uint64, isLog bool) bool {
			if isLog {
				return manifest.IsActiveLog(number)
			}
			return manifest.IsActiveTable(number)
		},
		KeepBlock: func(prevUserKey, userKey []byte) bool {
			blocksSeen.Add(1)
			if prevUserKey != nil && bytes.Compare(prevUserKey, wantHi) > 0 {
				return false
			}
			if bytes.Compare(wantLo, userKey) > 0 {
				return false
			}
			blocksKept.Add(1)
			return true
		},
		KeepRecord: func(userKey []byte) bool {
			return bytes.Compare(userKey, wantLo) >= 0 && bytes.Compare(userKey, wantHi) <= 0
		},
	})

	require.NoError(t, err)
	assert.Empty(t, damage, "obsolete corrupt table was skipped")
	assert.Positive(t, blocksSeen.Load())
	assert.Less(t, blocksKept.Load(), blocksSeen.Load(), "most blocks were skipped without decompression")
	require.Len(t, records, 10)
	assert.Equal(t, "k-050", string(records[0].UserKey()))
	assert.Equal(t, "k-059", string(records[9].UserKey()))
}
