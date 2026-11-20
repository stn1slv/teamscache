package leveldb

// Uvarint decodes a little-endian base-128 varint of at most maxBytes bytes.
// It returns the value and the number of bytes read, or 0 when data ends
// before the varint does.
//
// Like ccl_leveldb, it stops after maxBytes bytes even when the last
// byte has its continuation bit set.
func Uvarint(data []byte, maxBytes int) (value uint64, n int) {
	for i := 0; i < maxBytes; i++ {
		if i >= len(data) {
			return 0, 0
		}
		b := data[i]
		if i < 10 {
			value |= uint64(b&0x7f) << (7 * uint(i))
		}
		if b&0x80 == 0 {
			return value, i + 1
		}
	}
	return value, maxBytes
}
