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
	"hash/crc32"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// ErrCorrupt reports data that does not follow the LevelDB file formats.
var ErrCorrupt = errors.New("corrupt leveldb data")

// Damage describes a file section that could not be read.
type Damage struct {
	File   string `json:"file"`
	Offset int64  `json:"offset"`
	Length int64  `json:"length"`
	Reason string `json:"reason"`
}

var crcTable = crc32.MakeTable(crc32.Castagnoli)

func maskedCRC(data []byte) uint32 {
	crc := crc32.Checksum(data, crcTable)
	return (crc>>15 | crc<<17) + 0xa282ead8
}

// ContextReader stops reading when its context is canceled.
type ContextReader struct {
	Context context.Context
	Reader  io.Reader
}

// Read checks cancellation before each read.
func (r ContextReader) Read(p []byte) (int, error) {
	if err := r.Context.Err(); err != nil {
		return 0, err
	}
	return r.Reader.Read(p)
}

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

// ParseFileNumber parses the decimal number and kind of a LevelDB data file.
func ParseFileNumber(name string) (number uint64, isLog, ok bool) {
	if !IsDataFile(name) {
		return 0, false, false
	}
	stem := strings.TrimSuffix(name, filepath.Ext(name))
	number, err := strconv.ParseUint(stem, 10, 64)
	if err != nil {
		return 0, false, false
	}
	return number, strings.HasSuffix(name, ".log"), true
}

type dataFile struct {
	path   string
	number uint64
	isLog  bool
}

type fileWarning struct {
	msg  string
	args []any
}

func sectionWarning(msg, path string, offset, length int64, reason string) fileWarning {
	return fileWarning{
		msg:  msg,
		args: []any{"file", path, "offset", offset, "length", length, "reason", reason},
	}
}

type fileResult struct {
	records  []Record
	damage   []Damage
	warnings []fileWarning
	err      error
}

// ReadOptions controls which files, table blocks, and records ReadDirOptions
// reads. Nil callbacks accept everything. KeepBlock and KeepRecord may be
// called concurrently from multiple worker goroutines and must be safe for
// concurrent use.
type ReadOptions struct {
	// KeepFile reports whether a data file should be read.
	KeepFile func(number uint64, isLog bool) bool
	// KeepBlock reports whether a table data block should be decompressed and
	// decoded. prevUserKey is nil for the first block of a table; otherwise it
	// is the user key of the previous index entry. userKey is the user key of
	// the current index entry. Under the table's comparator, every key in the
	// block lies in [prevUserKey, userKey].
	KeepBlock func(prevUserKey, userKey []byte) bool
	// KeepRecord reports whether a decoded record should be retained.
	KeepRecord func(userKey []byte) bool
}

func parseDataFile(ctx context.Context, f dataFile, opts ReadOptions) fileResult {
	data, err := readFile(ctx, f.path)
	if err != nil {
		return fileResult{err: fmt.Errorf("read %s: %w", f.path, err)}
	}
	if f.isLog {
		recs, sections, err := parseLogFiltered(ctx, data, f.path, opts.KeepRecord)
		if err != nil {
			return fileResult{err: err}
		}
		res := fileResult{
			records:  recs,
			damage:   make([]Damage, 0, len(sections)),
			warnings: make([]fileWarning, 0, len(sections)),
		}
		for _, d := range sections {
			res.warnings = append(res.warnings, sectionWarning("skipped damaged log data", f.path, d.offset, d.length, d.reason))
			res.damage = append(res.damage, Damage{File: filepath.Base(f.path), Offset: d.offset, Length: d.length, Reason: d.reason})
		}
		return res
	}
	recs, sections, err := parseTableFiltered(ctx, data, f.path, opts)
	if ctx.Err() != nil {
		return fileResult{err: ctx.Err()}
	}
	var res fileResult
	res.records = recs
	if err != nil {
		res.warnings = append(res.warnings, fileWarning{
			msg:  "skipped the unreadable part of a table file",
			args: []any{"file", f.path, "records_read", len(recs), "error", err},
		})
		res.damage = append(res.damage, Damage{File: filepath.Base(f.path), Length: int64(len(data)), Reason: err.Error()})
	}
	for _, section := range sections {
		res.warnings = append(res.warnings, sectionWarning("skipped damaged table data", f.path, section.Offset, section.Length, section.Reason))
	}
	res.damage = append(res.damage, sections...)
	return res
}

func parseFiles(ctx context.Context, files []dataFile, parse func(context.Context, dataFile) fileResult) ([]fileResult, error) {
	results := make([]fileResult, len(files))
	if len(files) == 0 {
		return results, nil
	}
	workers := min(runtime.GOMAXPROCS(0), len(files))
	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var next atomic.Int64
	var errOnce sync.Once
	var firstErr error
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := int(next.Add(1) - 1)
				if i >= len(files) {
					return
				}
				if err := workCtx.Err(); err != nil {
					results[i] = fileResult{err: err}
					return
				}
				res := parse(workCtx, files[i])
				results[i] = res
				if res.err != nil {
					errOnce.Do(func() { firstErr = res.err })
					cancel()
					return
				}
			}
		}()
	}
	wg.Wait()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if firstErr != nil {
		return nil, firstErr
	}
	return results, nil
}

// ReadDir returns all records of the LevelDB in dir, in file-number order.
// Within a file, records keep their on-disk order.
//
// Damaged parts of a file are skipped with a warning, so one damaged file
// does not hide the rest of the data.
// It stops with the context's error when ctx is canceled.
func ReadDir(ctx context.Context, dir string, logger *slog.Logger) ([]Record, []Damage, error) {
	return ReadDirOptions(ctx, dir, logger, ReadOptions{})
}

// ReadDirOptions works like ReadDir, applying opts to filter files, table
// blocks, and records.
func ReadDirOptions(ctx context.Context, dir string, logger *slog.Logger, opts ReadOptions) ([]Record, []Damage, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, nil, fmt.Errorf("read leveldb directory: %w", err)
	}
	var files []dataFile
	for _, e := range entries {
		if !e.Type().IsRegular() {
			continue
		}
		name := e.Name()
		number, isLog, ok := ParseFileNumber(name)
		if !ok {
			continue
		}
		if opts.KeepFile != nil && !opts.KeepFile(number, isLog) {
			continue
		}
		files = append(files, dataFile{
			path:   filepath.Join(dir, name),
			number: number,
			isLog:  isLog,
		})
	}
	sort.SliceStable(files, func(i, j int) bool { return files[i].number < files[j].number })

	results, err := parseFiles(ctx, files, func(ctx context.Context, f dataFile) fileResult {
		return parseDataFile(ctx, f, opts)
	})
	if err != nil {
		return nil, nil, err
	}

	var totalRecords, totalDamage int
	for i := range results {
		if results[i].err != nil {
			return nil, nil, results[i].err
		}
		totalRecords += len(results[i].records)
		totalDamage += len(results[i].damage)
	}
	records := make([]Record, 0, totalRecords)
	damage := make([]Damage, 0, totalDamage)
	for i := range results {
		for _, w := range results[i].warnings {
			logger.Warn(w.msg, w.args...)
		}
		records = append(records, results[i].records...)
		damage = append(damage, results[i].damage...)
	}
	return records, damage, nil
}

func readFile(ctx context.Context, path string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }() // read-only, the read error is what matters
	return io.ReadAll(ContextReader{Context: ctx, Reader: file})
}
