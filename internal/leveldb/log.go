package leveldb

import (
	"bytes"
	"context"
	"encoding/binary"
)

const (
	logBlockSize  = 32768
	logHeaderSize = 7 // CRC (4), length (2), type (1)
)

// Log fragment types.
const (
	fragZero   = 0
	fragFull   = 1
	fragFirst  = 2
	fragMiddle = 3
	fragLast   = 4
)

type logDamage struct {
	offset int64
	length int64
	reason string
}

type logBatch struct {
	offset int64
	data   []byte
}

// readLogBatches joins the fragments of a log file into write batches.
//
// It follows LevelDB's own reader (db/log_reader.cc): when a block cannot be
// read, the rest of it is skipped and reading resumes at the next block.
// Zero-filled space ends a block quietly and is only reported when it cuts a
// record short or has data after it.
func readLogBatches(ctx context.Context, data []byte) ([]logBatch, []logDamage, error) {
	var (
		batches     []logBatch
		damage      []logDamage
		inRecord    bool
		recordStart int64
		dataOffset  int64
		fragments   [][]byte
	)
	skip := func(offset, length int64, reason string) {
		damage = append(damage, logDamage{offset: offset, length: length, reason: reason})
	}
	abandon := func(end int64, reason string) {
		if inRecord {
			skip(recordStart, end-recordStart, reason)
			inRecord = false
		}
	}

	for blockOffset := 0; blockOffset < len(data); blockOffset += logBlockSize {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		block := data[blockOffset:min(blockOffset+logBlockSize, len(data))]
		atEOF := len(block) < logBlockSize
		pos := 0
		for len(block)-pos >= logHeaderSize {
			headerOffset := int64(blockOffset + pos)
			length := int(binary.LittleEndian.Uint16(block[pos+4:]))
			fragType := block[pos+6]
			dataStart := pos + logHeaderSize

			if dataStart+length > len(block) {
				abandon(headerOffset, "record interrupted by damaged data")
				reason := "record length runs past the end of its block"
				if atEOF {
					reason = "incomplete record at the end of the file"
				}
				skip(headerOffset, int64(len(block)-pos), reason)
				break
			}
			if fragType == fragZero && length == 0 {
				abandon(headerOffset, "record interrupted by zero-filled space")
				if len(bytes.Trim(block[pos:], "\x00")) > 0 {
					skip(headerOffset, int64(len(block)-pos), "data after zero-filled space")
				}
				break
			}

			frag := block[dataStart : dataStart+length]
			if maskedCRC(block[pos+6:dataStart+length]) != binary.LittleEndian.Uint32(block[pos:]) {
				abandon(headerOffset, "record interrupted by a checksum mismatch")
				skip(headerOffset, int64(len(block)-pos), "checksum mismatch")
				break
			}
			pos = dataStart + length

			switch fragType {
			case fragFull:
				abandon(headerOffset, "record without its last fragment")
				batches = append(batches, logBatch{offset: int64(blockOffset + dataStart), data: frag})
			case fragFirst:
				abandon(headerOffset, "record without its last fragment")
				inRecord = true
				recordStart = headerOffset
				dataOffset = int64(blockOffset + dataStart)
				fragments = append(fragments[:0], frag) // earlier fragments were already joined
			case fragMiddle, fragLast:
				if !inRecord {
					skip(headerOffset, int64(logHeaderSize+length), "fragment without the start of its record")
					continue
				}
				fragments = append(fragments, frag)
				if fragType == fragLast {
					inRecord = false
					batches = append(batches, logBatch{offset: dataOffset, data: bytes.Join(fragments, nil)})
				}
			default:
				abandon(headerOffset, "record interrupted by damaged data")
				skip(headerOffset, int64(logHeaderSize+length), "unknown record type")
			}
		}
	}
	abandon(int64(len(data)), "incomplete record at the end of the file")
	return batches, damage, nil
}

// parseLog returns the records of a .log file and the stretches it skipped.
//
// A write batch is: sequence (8 bytes), count (4 bytes), then count entries of
// type (1 byte), varint-prefixed key and, for live entries, varint-prefixed
// value. Entry i of a batch gets sequence number seq+i.
func parseLog(ctx context.Context, data []byte, path string) ([]Record, []logDamage, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	batches, damage, err := readLogBatches(ctx, data)
	if err != nil {
		return nil, nil, err
	}
	var records []Record
	for _, b := range batches {
		if len(b.data) < 12 {
			damage = append(damage, logDamage{offset: b.offset, length: int64(len(b.data)), reason: "write batch shorter than its 12-byte header"})
			continue
		}
		seq := binary.LittleEndian.Uint64(b.data)
		count := binary.LittleEndian.Uint32(b.data[8:])
		pos := 12
		for i := uint32(0); i < count; i++ {
			if err := ctx.Err(); err != nil {
				return nil, nil, err
			}
			start := pos
			state, key, value, n := readBatchEntry(b.data[pos:])
			if n == 0 {
				damage = append(damage, logDamage{
					offset: b.offset + int64(start),
					length: int64(len(b.data) - start),
					reason: "write batch cut short",
				})
				break
			}
			pos += n
			records = append(records, Record{
				Key: key, Value: value, Seq: seq + uint64(i), State: state,
				FileType: Log, File: path, Offset: b.offset + int64(start),
			})
		}
	}
	return records, damage, nil
}

// readBatchEntry reads one write-batch entry. It returns n == 0 when the
// batch ends before the entry does or the entry type is not valid.
func readBatchEntry(data []byte) (state KeyState, key, value []byte, n int) {
	if len(data) < 1 || (data[0] != byte(Deleted) && data[0] != byte(Live)) {
		return 0, nil, nil, 0
	}
	state = KeyState(data[0])
	pos := 1
	key, m := lengthPrefixed(data[pos:])
	if m == 0 {
		return 0, nil, nil, 0
	}
	pos += m
	value = []byte{}
	if state != Deleted {
		value, m = lengthPrefixed(data[pos:])
		if m == 0 {
			return 0, nil, nil, 0
		}
		pos += m
	}
	return state, key, value, pos
}

func lengthPrefixed(data []byte) ([]byte, int) {
	length, n := Uvarint(data, 5)
	if n == 0 || length > uint64(len(data)-n) {
		return nil, 0
	}
	return data[n : n+int(length)], n + int(length)
}
