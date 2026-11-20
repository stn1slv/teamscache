package leveldb

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	goleveldb "github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
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

	records, err := ReadDir(context.Background(), dir, slog.New(slog.DiscardHandler))
	require.NoError(t, err)

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
	records, err := ReadDir(context.Background(), dir, slog.New(slog.NewTextHandler(&logs, nil)))

	require.NoError(t, err)
	assert.NotEmpty(t, records)
	assert.Contains(t, logs.String(), "999999.ldb")
}

func TestParseLogStopsAtZeroFill(t *testing.T) {
	t.Parallel()
	// One full fragment holding a batch with one live entry, then zeros.
	batch := []byte{1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, byte(Live), 1, 'k', 1, 'v'}
	data := make([]byte, logHeaderSize, logHeaderSize+len(batch)+100)
	data[4] = byte(len(batch))
	data[6] = fragFull
	data = append(data, batch...)
	data = append(data, make([]byte, 100)...)

	records, damage := parseLog(data, "x.log")

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
		return append(frag, batch...)
	}
	// A seven-digit file is read, and it comes after the six-digit one.
	require.NoError(t, os.WriteFile(filepath.Join(dir, "1000000.log"), logFile(2, "second"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "999999.log"), logFile(1, "first"), 0o600))

	records, err := ReadDir(context.Background(), dir, slog.New(slog.DiscardHandler))

	require.NoError(t, err)
	require.Len(t, records, 2)
	assert.Equal(t, "first", string(records[0].Key))
	assert.Equal(t, "second", string(records[1].Key))
}
