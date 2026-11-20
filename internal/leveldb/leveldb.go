// Package leveldb reads the raw records of a LevelDB directory without opening
// it as a database.
//
// It parses the table files (.ldb, .sst) and the write-ahead logs (.log) byte by
// byte and returns every record it finds, including old versions and deletions.
// It does not use a key comparator and does not merge records, so it can read a
// Chromium IndexedDB store, whose comparator ("idb_cmp1") other Go LevelDB
// libraries do not know.
//
// Ported from ccl_leveldb.py in ccl_chromium_reader (MIT, CCL Forensics).
// Format references:
// https://github.com/google/leveldb/blob/main/doc/table_format.md
// https://github.com/google/leveldb/blob/main/doc/log_format.md
package leveldb

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// ErrCorrupt reports data that does not follow the LevelDB file formats.
var ErrCorrupt = errors.New("corrupt leveldb data")

// KeyState is the state of a record: live (a put) or deleted (a tombstone).
type KeyState int

// Record states. The values match the LevelDB value types.
const (
	Deleted KeyState = 0
	Live    KeyState = 1
	Unknown KeyState = 2
)

// FileType is the kind of file a record came from.
type FileType int

// File types.
const (
	Ldb FileType = 1
	Log FileType = 2
)

// Record is one key and value found in a LevelDB file.
type Record struct {
	// Key is the raw key. For table records it still holds the 8-byte
	// sequence and type trailer; use UserKey for the key alone.
	Key      []byte
	Value    []byte
	Seq      uint64
	State    KeyState
	FileType FileType
	File     string
	Offset   int64
}

// UserKey returns the key without the trailer that table records carry.
func (r *Record) UserKey() []byte {
	if r.FileType == Ldb && len(r.Key) >= 8 {
		return r.Key[:len(r.Key)-8]
	}
	return r.Key
}

// LevelDB names data files with a decimal number of at least six digits.
var dataFilePattern = regexp.MustCompile(`^[0-9]{6,}\.(ldb|log|sst)$`)

// IsDataFile reports whether name is a LevelDB table or log file.
func IsDataFile(name string) bool {
	return dataFilePattern.MatchString(name)
}

type dataFile struct {
	path   string
	number uint64
	isLog  bool
}

// ReadDir returns all records of the LevelDB in dir, in file-number order.
// Within a file, records keep their on-disk order.
//
// Damaged parts of a file are skipped with a warning, so one damaged file
// does not hide the rest of the data.
// It stops with the context's error when ctx is canceled.
func ReadDir(ctx context.Context, dir string, logger *slog.Logger) ([]Record, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read leveldb directory: %w", err)
	}
	var files []dataFile
	for _, e := range entries {
		name := e.Name()
		if !e.Type().IsRegular() || !IsDataFile(name) {
			continue
		}
		stem := strings.TrimSuffix(name, filepath.Ext(name))
		number, err := strconv.ParseUint(stem, 10, 64)
		if err != nil {
			continue
		}
		files = append(files, dataFile{
			path:   filepath.Join(dir, name),
			number: number,
			isLog:  strings.HasSuffix(name, ".log"),
		})
	}
	sort.SliceStable(files, func(i, j int) bool { return files[i].number < files[j].number })

	var records []Record
	for _, f := range files {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		data, err := os.ReadFile(f.path)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", f.path, err)
		}
		if f.isLog {
			recs, damage := parseLog(data, f.path)
			for _, d := range damage {
				logger.Warn("skipped damaged log data", "file", f.path, "offset", d.offset, "length", d.length, "reason", d.reason)
			}
			records = append(records, recs...)
			continue
		}
		recs, err := parseTable(data, f.path)
		if err != nil {
			logger.Warn("skipped the unreadable part of a table file", "file", f.path, "records_read", len(recs), "error", err)
		}
		records = append(records, recs...)
	}
	return records, nil
}
