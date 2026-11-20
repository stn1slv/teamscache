package leveldb

import (
	"encoding/binary"
	"fmt"

	"github.com/golang/snappy"
)

const (
	tableFooterSize  = 48
	tableMagic       = 0xdb4775248b80fb57
	blockTrailerSize = 5 // 1 byte compression type, 4 bytes CRC (not checked)
)

// MaxDecodedSize is the largest Snappy output this module accepts. Larger
// sizes come from corrupt data, and decoding them would waste memory.
const MaxDecodedSize = 256 << 20

// DecodeSnappy decodes a Snappy block after checking its declared size.
func DecodeSnappy(src []byte) ([]byte, error) {
	n, err := snappy.DecodedLen(src)
	if err != nil {
		return nil, err
	}
	if n > MaxDecodedSize {
		return nil, fmt.Errorf("%w: snappy data declares %d bytes", ErrCorrupt, n)
	}
	return snappy.Decode(nil, src)
}

type blockHandle struct {
	offset uint64
	length uint64
}

func readBlockHandle(data []byte) (blockHandle, int, error) {
	offset, n1 := Uvarint(data, 10)
	if n1 <= 0 {
		return blockHandle{}, 0, fmt.Errorf("%w: bad block handle offset", ErrCorrupt)
	}
	length, n2 := Uvarint(data[n1:], 10)
	if n2 <= 0 {
		return blockHandle{}, 0, fmt.Errorf("%w: bad block handle length", ErrCorrupt)
	}
	return blockHandle{offset: offset, length: length}, n1 + n2, nil
}

// parseTable returns the records of a table file. On a format error it returns
// the records read so far together with the error.
func parseTable(data []byte, path string) ([]Record, error) {
	if len(data) < tableFooterSize {
		return nil, fmt.Errorf("%w: file shorter than the table footer", ErrCorrupt)
	}
	footer := data[len(data)-tableFooterSize:]
	if binary.LittleEndian.Uint64(footer[40:]) != tableMagic {
		return nil, fmt.Errorf("%w: bad table magic number", ErrCorrupt)
	}
	_, n, err := readBlockHandle(footer) // meta-index handle, not used
	if err != nil {
		return nil, err
	}
	indexHandle, _, err := readBlockHandle(footer[n:])
	if err != nil {
		return nil, err
	}
	index, _, err := readBlock(data, indexHandle)
	if err != nil {
		return nil, fmt.Errorf("index block: %w", err)
	}
	indexEntries, err := blockEntries(index)
	if err != nil {
		return nil, fmt.Errorf("index block: %w", err)
	}

	var records []Record
	for _, ie := range indexEntries {
		handle, _, err := readBlockHandle(ie.value)
		if err != nil {
			return records, err
		}
		block, compressed, err := readBlock(data, handle)
		if err != nil {
			return records, err
		}
		entries, err := blockEntries(block)
		for _, e := range entries {
			offset := int64(handle.offset)
			if !compressed {
				offset += int64(e.offset)
			}
			records = append(records, tableRecord(e.key, e.value, path, offset))
		}
		if err != nil {
			return records, err
		}
	}
	return records, nil
}

func tableRecord(key, value []byte, path string, offset int64) Record {
	r := Record{Key: key, Value: value, FileType: Ldb, File: path, Offset: offset, State: Unknown}
	if len(key) >= 8 {
		r.Seq = binary.LittleEndian.Uint64(key[len(key)-8:]) >> 8
	}
	if len(key) > 8 {
		if key[len(key)-8] == 0 {
			r.State = Deleted
		} else {
			r.State = Live
		}
	}
	return r
}

func readBlock(data []byte, h blockHandle) (block []byte, compressed bool, err error) {
	// Subtract instead of adding, so that a corrupt handle cannot overflow.
	size := uint64(len(data))
	if h.offset > size || size-h.offset < blockTrailerSize || h.length > size-h.offset-blockTrailerSize {
		return nil, false, fmt.Errorf("%w: block at offset %d runs past the end of the file", ErrCorrupt, h.offset)
	}
	raw := data[h.offset : h.offset+h.length]
	if data[h.offset+h.length] == 0 {
		return raw, false, nil
	}
	decoded, err := DecodeSnappy(raw)
	if err != nil {
		return nil, true, fmt.Errorf("%w: snappy block at offset %d: %w", ErrCorrupt, h.offset, err)
	}
	return decoded, true, nil
}

type blockEntry struct {
	key    []byte
	value  []byte
	offset int
}

// blockEntries decodes the prefix-compressed entries of a table block.
func blockEntries(block []byte) ([]blockEntry, error) {
	if len(block) < 4 {
		return nil, fmt.Errorf("%w: block too short", ErrCorrupt)
	}
	restartCount := uint64(binary.LittleEndian.Uint32(block[len(block)-4:]))
	if (restartCount+1)*4 > uint64(len(block)) {
		return nil, fmt.Errorf("%w: bad restart count", ErrCorrupt)
	}
	restartOffset := len(block) - int(restartCount+1)*4
	pos := 0
	if restartCount > 0 {
		first := uint64(binary.LittleEndian.Uint32(block[restartOffset:]))
		if first > uint64(restartOffset) {
			return nil, fmt.Errorf("%w: first restart point is past the entries", ErrCorrupt)
		}
		pos = int(first)
	}

	var entries []blockEntry
	var key []byte
	for pos < restartOffset {
		start := pos
		var vals [3]uint64
		for i := range vals {
			v, n := Uvarint(block[pos:restartOffset], 5)
			if n <= 0 {
				return entries, fmt.Errorf("%w: bad entry header at block offset %d", ErrCorrupt, start)
			}
			vals[i] = v
			pos += n
		}
		shared, nonShared, valueLen := vals[0], vals[1], vals[2]
		if shared > uint64(len(key)) {
			return entries, fmt.Errorf("%w: shared key length is larger than the previous key", ErrCorrupt)
		}
		if nonShared+valueLen > uint64(restartOffset-pos) {
			return entries, fmt.Errorf("%w: entry at block offset %d runs past the block", ErrCorrupt, start)
		}
		next := make([]byte, 0, int(shared+nonShared))
		next = append(next, key[:shared]...)
		next = append(next, block[pos:pos+int(nonShared)]...)
		key = next
		pos += int(nonShared)
		value := block[pos : pos+int(valueLen)]
		pos += int(valueLen)
		entries = append(entries, blockEntry{key: key, value: value, offset: start})
	}
	return entries, nil
}
