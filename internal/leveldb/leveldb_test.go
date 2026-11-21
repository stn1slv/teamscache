package leveldb

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/golang/snappy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	goleveldb "github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// writeFixture writes a LevelDB with goleveldb and returns the state a reader
// should see: the newest value of each live key.
func writeFixture(t *testing.T, dir string) map[string]string {
	t.Helper()
	db, err := goleveldb.OpenFile(dir, &opt.Options{WriteBuffer: 16 * 1024})
	require.NoError(t, err)

	want := map[string]string{}
	// Values long and repetitive enough to be stored Snappy-compressed.
	for i := range 2000 {
		key := fmt.Sprintf("key-%05d", i)
		value := strings.Repeat(fmt.Sprintf("value-%d;", i), 20)
		require.NoError(t, db.Put([]byte(key), []byte(value), nil))
		want[key] = value
	}
	// Overwrite and delete some keys, so old versions and tombstones exist.
	for i := 0; i < 2000; i += 7 {
		key := fmt.Sprintf("key-%05d", i)
		require.NoError(t, db.Put([]byte(key), []byte("new"), nil))
		want[key] = "new"
	}
	for i := 0; i < 2000; i += 11 {
		key := fmt.Sprintf("key-%05d", i)
		require.NoError(t, db.Delete([]byte(key), nil))
		delete(want, key)
	}
	// A large value is split over several log blocks.
	big := strings.Repeat("x", 3*logBlockSize)
	require.NoError(t, db.Put([]byte("big"), []byte(big), nil))
	want["big"] = big
	require.NoError(t, db.Close())
	return want
}

func newestLive(records []Record) map[string]string {
	type version struct {
		seq   uint64
		state KeyState
		value []byte
	}
	newest := map[string]version{}
	for _, r := range records {
		k := string(r.UserKey())
		if v, ok := newest[k]; !ok || r.Seq > v.seq {
			newest[k] = version{seq: r.Seq, state: r.State, value: r.Value}
		}
	}
	got := map[string]string{}
	for k, v := range newest {
		if v.state == Live {
			got[k] = string(v.value)
		}
	}
	return got
}

func TestReadDirMatchesGoleveldb(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	want := writeFixture(t, dir)

	records, damage, err := ReadDir(context.Background(), dir, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	assert.Empty(t, damage)

	var sawLog, sawTable, sawDeleted bool
	for _, r := range records {
		sawLog = sawLog || r.FileType == Log
		sawTable = sawTable || r.FileType == Ldb
		sawDeleted = sawDeleted || r.State == Deleted
	}
	assert.True(t, sawLog, "expected records from a .log file")
	assert.True(t, sawTable, "expected records from a table file")
	assert.True(t, sawDeleted, "expected tombstones")
	assert.Equal(t, want, newestLive(records))
}

func TestReadDirSkipsDamagedTable(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeFixture(t, dir)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "999999.ldb"), []byte("not a table"), 0o600))

	var logs bytes.Buffer
	records, damage, err := ReadDir(context.Background(), dir, slog.New(slog.NewTextHandler(&logs, nil)))

	require.NoError(t, err)
	assert.NotEmpty(t, records)
	assert.Contains(t, logs.String(), "999999.ldb")
	require.Len(t, damage, 1)
	assert.Equal(t, "999999.ldb", damage[0].File)
}

func TestParseLogStopsAtZeroFill(t *testing.T) {
	t.Parallel()
	// One full fragment holding a batch with one live entry, then zeros.
	batch := []byte{1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, byte(Live), 1, 'k', 1, 'v'}
	data := make([]byte, logHeaderSize, logHeaderSize+len(batch)+100)
	data[4] = byte(len(batch))
	data[6] = fragFull
	data = append(data, batch...)
	binary.LittleEndian.PutUint32(data, util.NewCRC(data[6:]).Value())
	data = append(data, make([]byte, 100)...)

	records, damage, err := parseLog(context.Background(), data, "x.log")

	require.NoError(t, err)
	require.Len(t, records, 1)
	assert.Equal(t, "k", string(records[0].Key))
	assert.Equal(t, "v", string(records[0].Value))
	assert.Equal(t, uint64(1), records[0].Seq)
	assert.Empty(t, damage)
}

func TestUvarint(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		data  []byte
		max   int
		value uint64
		n     int
	}{
		{"one byte", []byte{0x05}, 10, 5, 1},
		{"two bytes", []byte{0xac, 0x02}, 10, 300, 2},
		{"truncated", []byte{0x80}, 10, 0, 0},
		{"stops at limit", []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0x01}, 5, 0x7ffffffff, 5},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			value, n := Uvarint(tt.data, tt.max)
			assert.Equal(t, tt.value, value)
			assert.Equal(t, tt.n, n)
		})
	}
}

func TestReadBlockRejectsOverflowingHandle(t *testing.T) {
	t.Parallel()
	data := make([]byte, 64)

	_, _, err := readBlock(data, blockHandle{offset: 10, length: ^uint64(0) - 2})

	require.ErrorIs(t, err, ErrCorrupt)
}

func TestDecodeSnappyRejectsHugeSizes(t *testing.T) {
	t.Parallel()
	// A Snappy block starts with its decoded length as a varint: here 1 GiB.
	_, err := DecodeSnappy([]byte{0x80, 0x80, 0x80, 0x80, 0x04})

	require.ErrorIs(t, err, ErrCorrupt)
}

func TestDataFileNames(t *testing.T) {
	t.Parallel()
	assert.True(t, IsDataFile("000123.ldb"))
	assert.True(t, IsDataFile("1000000.log"), "file numbers can have more than six digits")
	assert.False(t, IsDataFile("12345.ldb"))
	assert.False(t, IsDataFile("MANIFEST-000001"))
	assert.False(t, IsDataFile("LOCK"))
}

func TestReadDirOrdersFilesByDecimalNumber(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	logFile := func(seq byte, key string) []byte {
		batch := make([]byte, 0, 16+len(key))
		batch = append(batch, seq, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, byte(Live), byte(len(key)))
		batch = append(batch, key...)
		batch = append(batch, 1, 'v')
		frag := make([]byte, logHeaderSize, logHeaderSize+len(batch))
		frag[4] = byte(len(batch))
		frag[6] = fragFull
		frag = append(frag, batch...)
		binary.LittleEndian.PutUint32(frag, util.NewCRC(frag[6:]).Value())
		return frag
	}
	// A seven-digit file is read, and it comes after the six-digit one.
	require.NoError(t, os.WriteFile(filepath.Join(dir, "1000000.log"), logFile(2, "second"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "999999.log"), logFile(1, "first"), 0o600))

	records, damage, err := ReadDir(context.Background(), dir, slog.New(slog.DiscardHandler))

	require.NoError(t, err)
	assert.Empty(t, damage)
	require.Len(t, records, 2)
	assert.Equal(t, "first", string(records[0].Key))
	assert.Equal(t, "second", string(records[1].Key))
}

func TestLogRejectsChecksumMismatchAndRecovers(t *testing.T) {
	t.Parallel()
	batch := []byte{1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, byte(Live), 1, 'k', 1, 'v'}
	fragment := make([]byte, logHeaderSize, logHeaderSize+len(batch))
	binary.LittleEndian.PutUint16(fragment[4:], uint16(len(batch)))
	fragment[6] = fragFull
	fragment = append(fragment, batch...)
	binary.LittleEndian.PutUint32(fragment, util.NewCRC(fragment[6:]).Value())
	data := make([]byte, logBlockSize, logBlockSize+len(fragment))
	copy(data, fragment)
	data[len(fragment)-1] ^= 1
	data = append(data, fragment...)

	records, damage, err := parseLog(context.Background(), data, "test.log")

	require.NoError(t, err)
	require.Len(t, records, 1, "only the intact batch in the next block is accepted")
	assert.Equal(t, "v", string(records[0].Value))
	require.Len(t, damage, 1)
	assert.Equal(t, "checksum mismatch", damage[0].reason)
}

func TestBlockChecksums(t *testing.T) {
	t.Parallel()
	for _, compression := range []byte{0, 1} {
		data := []byte("block data")
		if compression == 1 {
			data = snappy.Encode(nil, data)
		}
		length := len(data)
		data = append(data, compression)
		data = binary.LittleEndian.AppendUint32(data, util.NewCRC(data).Value())
		handle := blockHandle{length: uint64(length)}
		block, _, err := readBlock(data, handle)
		require.NoError(t, err)
		assert.Equal(t, "block data", string(block))
		data[0] ^= 1
		_, _, err = readBlock(data, handle)
		require.ErrorIs(t, err, ErrCorrupt)
	}
}

func TestParsersStopWhenCanceled(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err := parseLog(ctx, make([]byte, logBlockSize), "test.log")
	require.ErrorIs(t, err, context.Canceled)
	_, _, err = parseTable(ctx, make([]byte, tableFooterSize), "test.ldb")
	require.ErrorIs(t, err, context.Canceled)
	_, _, err = ReadDir(ctx, t.TempDir(), slog.New(slog.DiscardHandler))
	require.ErrorIs(t, err, context.Canceled)
}

func TestContextReaderStopsBetweenReads(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	reader := ContextReader{Context: ctx, Reader: strings.NewReader("abcdef")}
	buffer := make([]byte, 1)
	_, err := reader.Read(buffer)
	require.NoError(t, err)
	cancel()
	_, err = io.ReadAll(reader)
	require.ErrorIs(t, err, context.Canceled)
}

func TestUnknownBlockCompressionIsRejected(t *testing.T) {
	t.Parallel()
	data := []byte{'x', 2}
	data = binary.LittleEndian.AppendUint32(data, util.NewCRC(data).Value())
	_, _, err := readBlock(data, blockHandle{length: 1})
	require.ErrorIs(t, err, ErrCorrupt)
	assert.Contains(t, err.Error(), "unknown compression type")
}

func TestFragmentedLogChecksumFailureDropsTheWholeRecord(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	db, err := goleveldb.OpenFile(dir, nil)
	require.NoError(t, err)
	require.NoError(t, db.Put([]byte("big"), []byte(strings.Repeat("x", 3*logBlockSize)), nil))
	require.NoError(t, db.Close())
	files, err := filepath.Glob(filepath.Join(dir, "*.log"))
	require.NoError(t, err)
	require.Len(t, files, 1)
	data, err := os.ReadFile(files[0])
	require.NoError(t, err)
	records, damage, err := parseLog(context.Background(), data, files[0])
	require.NoError(t, err)
	require.Len(t, records, 1)
	assert.Empty(t, damage)
	for _, offset := range []int{logBlockSize, len(data) / logBlockSize * logBlockSize} {
		corrupt := append([]byte{}, data...)
		corrupt[offset+logHeaderSize] ^= 1
		records, damage, err = parseLog(context.Background(), corrupt, files[0])
		require.NoError(t, err)
		assert.Empty(t, records, "neither middle nor last fragment damage yields a partial record")
		reasons := make([]string, 0, len(damage))
		for _, section := range damage {
			reasons = append(reasons, section.reason)
		}
		assert.Contains(t, reasons, "checksum mismatch")
	}
}

func TestTableRecoveryKeepsBlocksAfterChecksumDamage(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	db, err := goleveldb.OpenFile(dir, &opt.Options{BlockSize: 128, Compression: opt.NoCompression})
	require.NoError(t, err)
	for i := range 80 {
		require.NoError(t, db.Put(fmt.Appendf(nil, "key-%03d", i), []byte(strings.Repeat("v", 40)), nil))
	}
	require.NoError(t, db.CompactRange(util.Range{}))
	require.NoError(t, db.Close())
	files, err := filepath.Glob(filepath.Join(dir, "*.ldb"))
	require.NoError(t, err)
	require.NotEmpty(t, files)
	data, err := os.ReadFile(files[len(files)-1])
	require.NoError(t, err)
	before, damage, err := parseTable(context.Background(), data, files[len(files)-1])
	require.NoError(t, err)
	assert.Empty(t, damage)
	require.NotEmpty(t, before)
	footer := data[len(data)-tableFooterSize:]
	_, n, err := readBlockHandle(footer)
	require.NoError(t, err)
	indexHandle, _, err := readBlockHandle(footer[n:])
	require.NoError(t, err)
	index, _, err := readBlock(data, indexHandle)
	require.NoError(t, err)
	entries, err := blockEntries(context.Background(), index)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(entries), 3)
	handle, _, err := readBlockHandle(entries[len(entries)/2].value)
	require.NoError(t, err)
	block, _, err := readBlock(data, handle)
	require.NoError(t, err)
	badEntries, err := blockEntries(context.Background(), block)
	require.NoError(t, err)
	data[handle.offset] ^= 1
	output := t.TempDir()
	// #nosec G703 -- The filename is fixed and output comes from t.TempDir.
	require.NoError(t, os.WriteFile(filepath.Join(output, "000001.ldb"), data, 0o600))

	after, damage, err := ReadDir(context.Background(), output, slog.New(slog.DiscardHandler))

	require.NoError(t, err)
	assert.Len(t, after, len(before)-len(badEntries))
	require.Len(t, damage, 1)
	assert.Equal(t, int64(handle.offset), damage[0].Offset)
	assert.Equal(t, int64(handle.length)+blockTrailerSize, damage[0].Length)
	require.NotEmpty(t, after)
	assert.Equal(t, before[0].UserKey(), after[0].UserKey())
	assert.Equal(t, before[len(before)-1].UserKey(), after[len(after)-1].UserKey())
}

func TestReadDirReturnsRootFileErrorInsteadOfCanceled(t *testing.T) {
	t.Parallel()
	if os.Geteuid() == 0 {
		t.Skip("root ignores file permissions")
	}
	dir := t.TempDir()
	writeFixture(t, dir)
	bad := filepath.Join(dir, "999999.ldb")
	require.NoError(t, os.WriteFile(bad, []byte("x"), 0o000))

	_, _, err := ReadDir(context.Background(), dir, slog.New(slog.DiscardHandler))

	require.Error(t, err)
	assert.NotErrorIs(t, err, context.Canceled)
}
