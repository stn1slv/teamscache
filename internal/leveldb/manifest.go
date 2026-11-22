package leveldb

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"
)

// LevelDB VersionEdit tags (db/version_edit.cc).
const (
	editComparator     = 1
	editLogNumber      = 2
	editNextFileNumber = 3
	editLastSequence   = 4
	editCompactPointer = 5
	editDeletedFile    = 6
	editNewFile        = 7
	editPrevLogNumber  = 9
)

// TableInfo describes one active SSTable listed in a LevelDB MANIFEST.
type TableInfo struct {
	Level    uint64
	Number   uint64
	Size     uint64
	Smallest []byte
	Largest  []byte
}

// SmallestUserKey returns Smallest without its 8-byte internal key trailer.
func (t TableInfo) SmallestUserKey() []byte {
	if len(t.Smallest) >= 8 {
		return t.Smallest[:len(t.Smallest)-8]
	}
	return t.Smallest
}

// LargestUserKey returns Largest without its 8-byte internal key trailer.
func (t TableInfo) LargestUserKey() []byte {
	if len(t.Largest) >= 8 {
		return t.Largest[:len(t.Largest)-8]
	}
	return t.Largest
}

// Manifest is the reconstructed state of a LevelDB MANIFEST file referenced by
// CURRENT.
type Manifest struct {
	File           string
	Comparator     string
	LogNumber      uint64
	PrevLogNumber  uint64
	NextFileNumber uint64
	LastSequence   uint64
	Tables         map[uint64]TableInfo
}

// IsActiveTable reports whether table file number is listed as live in the
// manifest.
func (m *Manifest) IsActiveTable(number uint64) bool {
	_, ok := m.Tables[number]
	return ok
}

// IsActiveLog reports whether log file number can contain uncommitted writes
// for the current manifest state.
func (m *Manifest) IsActiveLog(number uint64) bool {
	return number >= m.LogNumber || (m.PrevLogNumber != 0 && number == m.PrevLogNumber)
}

// ValidateManifestName checks that name is a safe MANIFEST filename within a
// LevelDB directory.
func ValidateManifestName(name string) error {
	if name == "" || name == "." || name == ".." || filepath.Base(name) != name || !strings.HasPrefix(name, "MANIFEST-") {
		return fmt.Errorf("%w: invalid CURRENT target %q", ErrCorrupt, name)
	}
	return nil
}

// ReadManifest reads CURRENT and replays the referenced MANIFEST file in dir.
// When CURRENT itself does not exist, it returns the underlying fs.ErrNotExist
// error. When CURRENT is invalid, points to a missing manifest, or the
// manifest is damaged or incomplete, it returns an error wrapping ErrCorrupt
// (and not fs.ErrNotExist).
func ReadManifest(ctx context.Context, dir string) (*Manifest, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	curBytes, err := readFile(ctx, filepath.Join(dir, "CURRENT"))
	if err != nil {
		return nil, err
	}
	name := strings.TrimSpace(string(curBytes))
	if err := ValidateManifestName(name); err != nil {
		return nil, err
	}
	data, err := readFile(ctx, filepath.Join(dir, name))
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if errors.Is(err, fs.ErrNotExist) {
			return nil, fmt.Errorf("%w: manifest %s referenced by CURRENT is missing", ErrCorrupt, name)
		}
		return nil, fmt.Errorf("%w: read manifest %s: %w", ErrCorrupt, name, err)
	}
	return ParseManifest(ctx, name, data)
}

type tableKey struct {
	level  uint64
	number uint64
}

type manifestReplay struct {
	manifest          Manifest
	byLevel           map[tableKey]TableInfo
	hasLogNumber      bool
	hasNextFileNumber bool
	hasLastSequence   bool
}

// ParseManifest replays raw MANIFEST file bytes into a Manifest.
func ParseManifest(ctx context.Context, name string, data []byte) (*Manifest, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := ValidateManifestName(name); err != nil {
		return nil, err
	}
	batches, damage, err := readLogBatches(ctx, data)
	if err != nil {
		return nil, err
	}
	if len(damage) > 0 {
		return nil, fmt.Errorf("%w: manifest %s has %d damaged section(s)", ErrCorrupt, name, len(damage))
	}
	if len(batches) == 0 {
		return nil, fmt.Errorf("%w: manifest %s has no edits", ErrCorrupt, name)
	}
	replay := manifestReplay{
		manifest: Manifest{File: name},
		byLevel:  map[tableKey]TableInfo{},
	}
	for _, b := range batches {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := applyVersionEdit(&replay, b.data); err != nil {
			return nil, fmt.Errorf("%w: manifest %s: %w", ErrCorrupt, name, err)
		}
	}
	if !replay.hasLogNumber || !replay.hasNextFileNumber || !replay.hasLastSequence {
		return nil, fmt.Errorf("%w: manifest %s is missing required metadata", ErrCorrupt, name)
	}
	replay.manifest.Tables = make(map[uint64]TableInfo, len(replay.byLevel))
	for _, info := range replay.byLevel {
		replay.manifest.Tables[info.Number] = info
	}
	return &replay.manifest, nil
}

func applyVersionEdit(r *manifestReplay, data []byte) error {
	m := &r.manifest
	pos := 0
	for pos < len(data) {
		tag, n := Uvarint(data[pos:], 5)
		if n == 0 {
			return fmt.Errorf("truncated version edit tag at offset %d", pos)
		}
		pos += n
		switch tag {
		case editComparator:
			raw, k := lengthPrefixed(data[pos:])
			if k == 0 {
				return fmt.Errorf("truncated comparator at offset %d", pos)
			}
			m.Comparator = string(raw)
			pos += k
		case editLogNumber:
			v, k := Uvarint(data[pos:], 10)
			if k == 0 {
				return fmt.Errorf("truncated log number at offset %d", pos)
			}
			m.LogNumber = v
			r.hasLogNumber = true
			pos += k
		case editPrevLogNumber:
			v, k := Uvarint(data[pos:], 10)
			if k == 0 {
				return fmt.Errorf("truncated prev log number at offset %d", pos)
			}
			m.PrevLogNumber = v
			pos += k
		case editNextFileNumber:
			v, k := Uvarint(data[pos:], 10)
			if k == 0 {
				return fmt.Errorf("truncated next file number at offset %d", pos)
			}
			m.NextFileNumber = v
			r.hasNextFileNumber = true
			pos += k
		case editLastSequence:
			v, k := Uvarint(data[pos:], 10)
			if k == 0 {
				return fmt.Errorf("truncated last sequence at offset %d", pos)
			}
			m.LastSequence = v
			r.hasLastSequence = true
			pos += k
		case editCompactPointer:
			_, k1 := Uvarint(data[pos:], 5)
			if k1 == 0 {
				return fmt.Errorf("truncated compact pointer level at offset %d", pos)
			}
			pos += k1
			_, k2 := lengthPrefixed(data[pos:])
			if k2 == 0 {
				return fmt.Errorf("truncated compact pointer key at offset %d", pos)
			}
			pos += k2
		case editDeletedFile:
			level, k1 := Uvarint(data[pos:], 5)
			if k1 == 0 {
				return fmt.Errorf("truncated deleted file level at offset %d", pos)
			}
			pos += k1
			fileNo, k2 := Uvarint(data[pos:], 10)
			if k2 == 0 {
				return fmt.Errorf("truncated deleted file number at offset %d", pos)
			}
			pos += k2
			delete(r.byLevel, tableKey{level: level, number: fileNo})
		case editNewFile:
			level, k1 := Uvarint(data[pos:], 5)
			if k1 == 0 {
				return fmt.Errorf("truncated new file level at offset %d", pos)
			}
			pos += k1
			fileNo, k2 := Uvarint(data[pos:], 10)
			if k2 == 0 {
				return fmt.Errorf("truncated new file number at offset %d", pos)
			}
			pos += k2
			size, k3 := Uvarint(data[pos:], 10)
			if k3 == 0 {
				return fmt.Errorf("truncated new file size at offset %d", pos)
			}
			pos += k3
			smallest, k4 := lengthPrefixed(data[pos:])
			if k4 == 0 {
				return fmt.Errorf("truncated new file smallest key at offset %d", pos)
			}
			pos += k4
			largest, k5 := lengthPrefixed(data[pos:])
			if k5 == 0 {
				return fmt.Errorf("truncated new file largest key at offset %d", pos)
			}
			pos += k5
			r.byLevel[tableKey{level: level, number: fileNo}] = TableInfo{
				Level:    level,
				Number:   fileNo,
				Size:     size,
				Smallest: append([]byte(nil), smallest...),
				Largest:  append([]byte(nil), largest...),
			}
		default:
			return fmt.Errorf("unknown version edit tag %d", tag)
		}
	}
	return nil
}
