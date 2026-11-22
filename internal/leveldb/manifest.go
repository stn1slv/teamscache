package leveldb

import (
	"context"
	"fmt"
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

// ReadManifest reads CURRENT and replays the referenced MANIFEST file in dir.
// It returns an error wrapping ErrCorrupt when CURRENT or the manifest file is
// invalid or damaged.
func ReadManifest(ctx context.Context, dir string) (*Manifest, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	curBytes, err := readFile(ctx, filepath.Join(dir, "CURRENT"))
	if err != nil {
		return nil, err
	}
	name := strings.TrimSpace(string(curBytes))
	if name == "" || name == "." || name == ".." || filepath.Base(name) != name {
		return nil, fmt.Errorf("%w: invalid CURRENT target %q", ErrCorrupt, name)
	}
	data, err := readFile(ctx, filepath.Join(dir, name))
	if err != nil {
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
	m := &Manifest{File: name, Tables: map[uint64]TableInfo{}}
	for _, b := range batches {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := applyVersionEdit(m, b.data); err != nil {
			return nil, fmt.Errorf("%w: manifest %s: %w", ErrCorrupt, name, err)
		}
	}
	return m, nil
}

func applyVersionEdit(m *Manifest, data []byte) error {
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
			pos += k
		case editLastSequence:
			v, k := Uvarint(data[pos:], 10)
			if k == 0 {
				return fmt.Errorf("truncated last sequence at offset %d", pos)
			}
			m.LastSequence = v
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
			_, k1 := Uvarint(data[pos:], 5)
			if k1 == 0 {
				return fmt.Errorf("truncated deleted file level at offset %d", pos)
			}
			pos += k1
			fileNo, k2 := Uvarint(data[pos:], 10)
			if k2 == 0 {
				return fmt.Errorf("truncated deleted file number at offset %d", pos)
			}
			pos += k2
			delete(m.Tables, fileNo)
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
			m.Tables[fileNo] = TableInfo{
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
