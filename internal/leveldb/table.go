package leveldb

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"path/filepath"

	"github.com/golang/snappy"
)

const (
	tableFooterSize  = 48
	tableMagic       = 0xdb4775248b80fb57
	blockTrailerSize = 5 // 1 byte compression type, 4 bytes masked CRC32C
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

// parseTable skips damaged data blocks and continues through the index.
// An unreadable footer or index is a file-level error.
func parseTable(ctx context.Context, data []byte, path string) ([]Record, []Damage, error) {
	return parseTableFiltered(ctx, data, path, ReadOptions{})
}

func parseTableFiltered(ctx context.Context, data []byte, path string, opts ReadOptions) ([]Record, []Damage, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if len(data) < tableFooterSize {
		return nil, nil, fmt.Errorf("%w: file shorter than the table footer", ErrCorrupt)
	}
	footer := data[len(data)-tableFooterSize:]
	return parseTableCore(ctx, int64(len(data)), footer, path, opts, func(h blockHandle) ([]byte, bool, error) {
		return readBlock(data, h)
	})
}

func parseTableAt(ctx context.Context, r io.ReaderAt, size int64, path string, opts ReadOptions) ([]Record, []Damage, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if size < tableFooterSize {
		return nil, nil, fmt.Errorf("%w: file shorter than the table footer", ErrCorrupt)
	}
	var footer [tableFooterSize]byte
	if err := readExactAt(ctx, r, footer[:], size-tableFooterSize); err != nil {
		return nil, nil, err
	}
	return parseTableCore(ctx, size, footer[:], path, opts, func(h blockHandle) ([]byte, bool, error) {
		return readBlockAt(ctx, r, size, h)
	})
}

func parseTableCore(
	ctx context.Context,
	size int64,
	footer []byte,
	path string,
	opts ReadOptions,
	readBlockFn func(blockHandle) ([]byte, bool, error),
) ([]Record, []Damage, error) {
	if binary.LittleEndian.Uint64(footer[40:]) != tableMagic {
		return nil, nil, fmt.Errorf("%w: bad table magic number", ErrCorrupt)
	}
	_, n, err := readBlockHandle(footer) // meta-index handle, not used
	if err != nil {
		return nil, nil, err
	}
	indexHandle, _, err := readBlockHandle(footer[n:])
	if err != nil {
		return nil, nil, err
	}
	index, _, err := readBlockFn(indexHandle)
	if err != nil {
		if ctx.Err() != nil {
			return nil, nil, ctx.Err()
		}
		if !errors.Is(err, ErrCorrupt) {
			return nil, nil, err
		}
		return nil, []Damage{blockDamage(size, path, indexHandle, err)}, nil
	}
	indexEntries, err := blockEntries(ctx, index)
	damage := []Damage{}
	if err != nil {
		if ctx.Err() != nil {
			return nil, nil, ctx.Err()
		}
		damage = append(damage, blockDamage(size, path, indexHandle, err))
	}

	var records []Record
	var prevSep []byte
	for _, ie := range indexEntries {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		var curSep []byte
		if len(ie.key) >= 8 {
			curSep = ie.key[:len(ie.key)-8]
		}
		if opts.KeepBlock != nil && curSep != nil && !opts.KeepBlock(prevSep, curSep) {
			prevSep = curSep
			continue
		}
		if curSep != nil {
			prevSep = curSep
		}

		handle, _, err := readBlockHandle(ie.value)
		if err != nil {
			damage = append(damage, blockDamage(size, path, indexHandle, err))
			continue
		}
		block, compressed, err := readBlockFn(handle)
		if err != nil {
			if ctx.Err() != nil {
				return nil, nil, ctx.Err()
			}
			if !errors.Is(err, ErrCorrupt) {
				return nil, nil, err
			}
			damage = append(damage, blockDamage(size, path, handle, err))
			continue
		}
		entries, err := blockEntries(ctx, block)
		for _, e := range entries {
			if opts.KeepRecord != nil {
				uk := e.key
				if len(uk) >= 8 {
					uk = uk[:len(uk)-8]
				}
				if !opts.KeepRecord(uk) {
					continue
				}
			}
			offset := int64(handle.offset)
			if !compressed {
				offset += int64(e.offset)
			}
			records = append(records, tableRecord(e.key, e.value, path, offset))
		}
		if err != nil {
			if ctx.Err() != nil {
				return nil, nil, ctx.Err()
			}
			damage = append(damage, blockDamage(size, path, handle, err))
		}
	}
	return records, damage, nil
}

func blockDamage(fileSize int64, path string, handle blockHandle, err error) Damage {
	size := uint64(max(fileSize, 0))
	offset := min(handle.offset, size)
	length := min(handle.length, size-offset)
	length += min(uint64(blockTrailerSize), size-offset-length)
	return Damage{
		File: filepath.Base(path), Offset: int64(offset), Length: int64(length), Reason: err.Error(),
	}
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

func readExactAt(ctx context.Context, r io.ReaderAt, buf []byte, offset int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	for done := 0; done < len(buf); {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, err := r.ReadAt(buf[done:], offset+int64(done))
		done += n
		if err != nil {
			if errors.Is(err, io.EOF) {
				if done == len(buf) {
					return nil
				}
				return io.ErrUnexpectedEOF
			}
			return err
		}
	}
	return nil
}

func readBlock(data []byte, h blockHandle) (block []byte, compressed bool, err error) {
	// Subtract instead of adding, so that a corrupt handle cannot overflow.
	size := uint64(len(data))
	if h.offset > size || size-h.offset < blockTrailerSize || h.length > size-h.offset-blockTrailerSize {
		return nil, false, fmt.Errorf("%w: block at offset %d runs past the end of the file", ErrCorrupt, h.offset)
	}
	raw := data[h.offset : h.offset+h.length]
	end := h.offset + h.length
	if maskedCRC(data[h.offset:end+1]) != binary.LittleEndian.Uint32(data[end+1:end+5]) {
		return nil, false, fmt.Errorf("%w: checksum mismatch at block offset %d", ErrCorrupt, h.offset)
	}
	if data[end] == 0 {
		return raw, false, nil
	}
	if data[end] != 1 {
		return nil, false, fmt.Errorf("%w: unknown compression type %d", ErrCorrupt, data[end])
	}
	decoded, err := DecodeSnappy(raw)
	if err != nil {
		return nil, true, fmt.Errorf("%w: snappy block at offset %d: %w", ErrCorrupt, h.offset, err)
	}
	return decoded, true, nil
}

func readBlockAt(ctx context.Context, r io.ReaderAt, fileSize int64, h blockHandle) (block []byte, compressed bool, err error) {
	// Subtract instead of adding, so that a corrupt handle cannot overflow.
	if fileSize < 0 {
		return nil, false, fmt.Errorf("%w: negative table size", ErrCorrupt)
	}
	size := uint64(fileSize)
	if h.offset > size || size-h.offset < blockTrailerSize || h.length > size-h.offset-blockTrailerSize {
		return nil, false, fmt.Errorf("%w: block at offset %d runs past the end of the file", ErrCorrupt, h.offset)
	}
	if h.length > MaxDecodedSize {
		return nil, false, fmt.Errorf("%w: block at offset %d declares %d bytes", ErrCorrupt, h.offset, h.length)
	}
	buf := make([]byte, int(h.length)+blockTrailerSize)
	if err := readExactAt(ctx, r, buf, int64(h.offset)); err != nil {
		return nil, false, err
	}
	raw := buf[:h.length]
	compType := buf[h.length]
	if maskedCRC(buf[:h.length+1]) != binary.LittleEndian.Uint32(buf[h.length+1:]) {
		return nil, false, fmt.Errorf("%w: checksum mismatch at block offset %d", ErrCorrupt, h.offset)
	}
	if compType == 0 {
		return raw, false, nil
	}
	if compType != 1 {
		return nil, false, fmt.Errorf("%w: unknown compression type %d", ErrCorrupt, compType)
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
func blockEntries(ctx context.Context, block []byte) ([]blockEntry, error) {
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
		if err := ctx.Err(); err != nil {
			return nil, err
		}
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
