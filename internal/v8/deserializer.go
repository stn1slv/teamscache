package v8

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"unicode/utf16"
	"unicode/utf8"
)

var (
	// ErrInvalid reports data that is not a valid serialized value.
	ErrInvalid = errors.New("invalid v8 serialized data")
	// ErrUnsupported reports a valid tag this package does not decode.
	ErrUnsupported = errors.New("unsupported v8 serialized data")
)

// Serialization tags.
const (
	tagVersion           = 0xFF
	tagPadding           = 0x00
	tagTheHole           = '-'
	tagUndefined         = '_'
	tagNull              = '0'
	tagTrue              = 'T'
	tagFalse             = 'F'
	tagInt32             = 'I'
	tagUint32            = 'U'
	tagDouble            = 'N'
	tagBigInt            = 'Z'
	tagUtf8String        = 'S'
	tagOneByteString     = '"'
	tagTwoByteString     = 'c'
	tagObjectReference   = '^'
	tagBeginJSObject     = 'o'
	tagEndJSObject       = '{'
	tagBeginSparseArray  = 'a'
	tagEndSparseArray    = '@'
	tagBeginDenseArray   = 'A'
	tagEndDenseArray     = '$'
	tagDate              = 'D'
	tagTrueObject        = 'y'
	tagFalseObject       = 'x'
	tagNumberObject      = 'n'
	tagBigIntObject      = 'z'
	tagStringObject      = 's'
	tagRegExp            = 'R'
	tagBeginJSMap        = ';'
	tagEndJSMap          = ':'
	tagBeginJSSet        = '\''
	tagEndJSSet          = ','
	tagArrayBuffer       = 'B'
	tagArrayBufferView   = 'V'
	tagHostObject        = '\\'
	tagEndOfData         = -1
	maxNestingDepth      = 10000
	versionWithViewFlags = 14
)

// HostObjectReader decodes the data after a host object tag.
type HostObjectReader func(d *Deserializer) (any, error)

// maxSlots is the number of array, list and typed array elements one value
// may allocate in total. It stops small corrupt values from claiming large
// amounts of memory through many arrays.
const maxSlots = 1 << 22

// Deserializer reads one value from V8 serialized data.
type Deserializer struct {
	data    []byte
	pos     int
	version uint64
	objects []any
	host    HostObjectReader
	depth   int
	slots   uint64 // elements allocated so far
}

// reserve counts n more allocated elements against the budget of one value.
func (d *Deserializer) reserve(n uint64) error {
	if n > maxSlots || d.slots+n > maxSlots {
		return fmt.Errorf("%w: value allocates more than %d elements", ErrUnsupported, maxSlots)
	}
	d.slots += n
	return nil
}

// remaining is the number of bytes not read yet.
func (d *Deserializer) remaining() uint64 { return uint64(len(d.data) - d.pos) }

// Deserialize decodes the value in data, which starts with the V8 version
// header. host decodes host objects; it may be nil when none are expected.
// Data after the value is ignored.
func Deserialize(data []byte, host HostObjectReader) (any, error) {
	d := &Deserializer{data: data, host: host}
	if d.readTag() != tagVersion {
		return nil, fmt.Errorf("%w: missing version header", ErrInvalid)
	}
	version, err := d.ReadVarint()
	if err != nil {
		return nil, err
	}
	d.version = version
	return d.readObject()
}

// ReadVarint reads an unsigned varint.
func (d *Deserializer) ReadVarint() (uint64, error) {
	return d.readVarint(10)
}

// ReadVarint32 reads an unsigned varint of at most 5 bytes.
func (d *Deserializer) ReadVarint32() (uint64, error) {
	return d.readVarint(5)
}

func (d *Deserializer) readVarint(maxBytes int) (uint64, error) {
	var value uint64
	for i := 0; i < maxBytes; i++ {
		if d.pos >= len(d.data) {
			return 0, fmt.Errorf("%w: data ends inside a varint", ErrInvalid)
		}
		b := d.data[d.pos]
		d.pos++
		if i < 10 {
			value |= uint64(b&0x7f) << (7 * uint(i))
		}
		if b&0x80 == 0 {
			break
		}
	}
	return value, nil
}

// ReadRaw reads n bytes.
func (d *Deserializer) ReadRaw(n uint64) ([]byte, error) {
	if n > uint64(len(d.data)-d.pos) {
		return nil, fmt.Errorf("%w: wanted %d bytes at offset %d, %d left", ErrInvalid, n, d.pos, len(d.data)-d.pos)
	}
	raw := d.data[d.pos : d.pos+int(n)]
	d.pos += int(n)
	return raw, nil
}

// ReadByte reads one byte.
func (d *Deserializer) ReadByte() (byte, error) {
	raw, err := d.ReadRaw(1)
	if err != nil {
		return 0, err
	}
	return raw[0], nil
}

// readTag returns the next tag, skipping padding, or tagEndOfData.
func (d *Deserializer) readTag() int {
	for d.pos < len(d.data) {
		t := d.data[d.pos]
		d.pos++
		if t != tagPadding {
			return int(t)
		}
	}
	return tagEndOfData
}

func (d *Deserializer) peekTag() int {
	start := d.pos
	t := d.readTag()
	d.pos = start
	return t
}

func (d *Deserializer) readDouble() (float64, error) {
	raw, err := d.ReadRaw(8)
	if err != nil {
		return 0, err
	}
	return math.Float64frombits(binary.LittleEndian.Uint64(raw)), nil
}

func (d *Deserializer) readObject() (any, error) {
	d.depth++
	defer func() { d.depth-- }()
	if d.depth > maxNestingDepth {
		return nil, fmt.Errorf("%w: nesting too deep", ErrInvalid)
	}
	value, err := d.readObjectInternal()
	if err != nil {
		return nil, err
	}
	if d.peekTag() == tagArrayBufferView {
		d.readTag()
		return d.readArrayBufferView(value)
	}
	return value, nil
}

func (d *Deserializer) readObjectInternal() (any, error) {
	tag := d.readTag()
	switch tag {
	case tagUndefined, tagTheHole:
		return Undefined{}, nil
	case tagNull:
		return nil, nil
	case tagTrue:
		return true, nil
	case tagFalse:
		return false, nil
	case tagTrueObject, tagFalseObject:
		v := tag == tagTrueObject
		d.objects = append(d.objects, v)
		return v, nil
	case tagInt32:
		u, err := d.ReadVarint()
		if err != nil {
			return nil, err
		}
		// ZigZag decoding.
		return int64(u>>1) ^ -int64(u&1), nil
	case tagUint32:
		u, err := d.ReadVarint()
		if err != nil {
			return nil, err
		}
		return int64(u), nil
	case tagDouble:
		return d.readDouble()
	case tagNumberObject:
		v, err := d.readDouble()
		if err != nil {
			return nil, err
		}
		d.objects = append(d.objects, v)
		return v, nil
	case tagBigInt:
		return d.readBigInt()
	case tagBigIntObject:
		v, err := d.readBigInt()
		if err != nil {
			return nil, err
		}
		d.objects = append(d.objects, v)
		return v, nil
	case tagUtf8String:
		return d.readUTF8String()
	case tagOneByteString:
		return d.readOneByteString()
	case tagTwoByteString:
		return d.readTwoByteString()
	case tagStringObject:
		v, err := d.readString()
		if err != nil {
			return nil, err
		}
		d.objects = append(d.objects, v)
		return v, nil
	case tagDate:
		v, err := d.readDouble()
		if err != nil {
			return nil, err
		}
		date := Date{Millis: v}
		d.objects = append(d.objects, date)
		return date, nil
	case tagRegExp:
		return d.readRegExp()
	case tagObjectReference:
		id, err := d.ReadVarint()
		if err != nil {
			return nil, err
		}
		if id >= uint64(len(d.objects)) {
			return nil, fmt.Errorf("%w: reference to unknown object %d", ErrInvalid, id)
		}
		return d.objects[id], nil
	case tagBeginJSObject:
		return d.readJSObject()
	case tagBeginSparseArray:
		return d.readSparseArray()
	case tagBeginDenseArray:
		return d.readDenseArray()
	case tagBeginJSMap:
		return d.readJSMap()
	case tagBeginJSSet:
		return d.readJSSet()
	case tagArrayBuffer:
		n, err := d.ReadVarint()
		if err != nil {
			return nil, err
		}
		raw, err := d.ReadRaw(n)
		if err != nil {
			return nil, err
		}
		buf := append([]byte(nil), raw...)
		d.objects = append(d.objects, buf)
		return buf, nil
	case tagHostObject:
		if d.host == nil {
			return nil, fmt.Errorf("%w: host object without a host object reader", ErrUnsupported)
		}
		v, err := d.host(d)
		if err != nil {
			return nil, err
		}
		d.objects = append(d.objects, v)
		return v, nil
	case tagEndOfData:
		return nil, fmt.Errorf("%w: data ends where a value was expected", ErrInvalid)
	default:
		return nil, fmt.Errorf("%w: tag %q at offset %d", ErrUnsupported, rune(tag), d.pos-1)
	}
}

func (d *Deserializer) readBigInt() (*big.Int, error) {
	bitfield, err := d.ReadVarint()
	if err != nil {
		return nil, err
	}
	// Bit 0 is the sign; the other bits are the length in bytes.
	raw, err := d.ReadRaw(bitfield >> 1)
	if err != nil {
		return nil, err
	}
	// Digits are little-endian; big.Int wants big-endian bytes.
	be := make([]byte, len(raw))
	for i, b := range raw {
		be[len(raw)-1-i] = b
	}
	v := new(big.Int).SetBytes(be)
	if bitfield&1 != 0 {
		v.Neg(v)
	}
	return v, nil
}

func (d *Deserializer) readUTF8String() (string, error) {
	n, err := d.ReadVarint()
	if err != nil {
		return "", err
	}
	raw, err := d.ReadRaw(n)
	if err != nil {
		return "", err
	}
	if !utf8.Valid(raw) {
		return "", fmt.Errorf("%w: invalid UTF-8 string", ErrInvalid)
	}
	return string(raw), nil
}

// readOneByteString decodes a one-byte string, which V8 stores as Latin-1.
func (d *Deserializer) readOneByteString() (string, error) {
	n, err := d.ReadVarint()
	if err != nil {
		return "", err
	}
	raw, err := d.ReadRaw(n)
	if err != nil {
		return "", err
	}
	runes := make([]rune, len(raw))
	for i, b := range raw {
		runes[i] = rune(b)
	}
	return string(runes), nil
}

// readTwoByteString decodes a UTF-16LE string. A lone surrogate becomes
// U+FFFD.
func (d *Deserializer) readTwoByteString() (string, error) {
	n, err := d.ReadVarint()
	if err != nil {
		return "", err
	}
	raw, err := d.ReadRaw(n)
	if err != nil {
		return "", err
	}
	if len(raw)%2 != 0 {
		return "", fmt.Errorf("%w: two-byte string with an odd length", ErrInvalid)
	}
	units := make([]uint16, len(raw)/2)
	for i := range units {
		units[i] = binary.LittleEndian.Uint16(raw[2*i:])
	}
	return string(utf16.Decode(units)), nil
}

func (d *Deserializer) readString() (string, error) {
	if d.version < 12 {
		return d.readUTF8String()
	}
	v, err := d.readObject()
	if err != nil {
		return "", err
	}
	s, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("%w: expected a string, got %T", ErrInvalid, v)
	}
	return s, nil
}

func (d *Deserializer) readRegExp() (any, error) {
	pattern, err := d.readString()
	if err != nil {
		return nil, err
	}
	flags, err := d.ReadVarint()
	if err != nil {
		return nil, err
	}
	re := RegExp{Pattern: pattern, Flags: flags}
	d.objects = append(d.objects, re)
	return re, nil
}

// readProperties reads key/value pairs until endTag and consumes endTag.
func (d *Deserializer) readProperties(endTag int, add func(key, value any) error) error {
	for {
		switch d.peekTag() {
		case endTag:
			d.readTag()
			return nil
		case tagEndOfData:
			return fmt.Errorf("%w: data ends inside an object", ErrInvalid)
		}
		key, err := d.readObject()
		if err != nil {
			return err
		}
		value, err := d.readObject()
		if err != nil {
			return err
		}
		if err := add(key, value); err != nil {
			return err
		}
	}
}

func (d *Deserializer) readJSObject() (any, error) {
	obj := NewObject()
	d.objects = append(d.objects, obj)
	if err := d.readProperties(tagEndJSObject, obj.add); err != nil {
		return nil, err
	}
	count, err := d.ReadVarint()
	if err != nil {
		return nil, err
	}
	if count != uint64(obj.Len()) {
		return nil, fmt.Errorf("%w: object has %d properties, header says %d", ErrInvalid, obj.Len(), count)
	}
	return obj, nil
}

// arrayIndex converts a sparse property key to an index, as ccl does.
func arrayIndex(key any, length int) (int, error) {
	var i int64
	switch k := key.(type) {
	case int64:
		i = k
	case float64:
		i = int64(k)
	case string:
		v, err := strconv.ParseInt(k, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("%w: array property %q is not an index", ErrUnsupported, k)
		}
		i = v
	default:
		return 0, fmt.Errorf("%w: array property of type %T", ErrUnsupported, key)
	}
	if i < 0 || i >= int64(length) {
		return 0, fmt.Errorf("%w: array index %d out of range", ErrUnsupported, i)
	}
	return int(i), nil
}

func (d *Deserializer) readArrayTail(arr []any, endTag int) error {
	props := 0
	err := d.readProperties(endTag, func(key, value any) error {
		i, err := arrayIndex(key, len(arr))
		if err != nil {
			return err
		}
		arr[i] = value
		props++
		return nil
	})
	if err != nil {
		return err
	}
	count, err := d.ReadVarint()
	if err != nil {
		return err
	}
	if count != uint64(props) {
		return fmt.Errorf("%w: array has %d properties, header says %d", ErrInvalid, props, count)
	}
	_, err = d.ReadVarint() // length, not checked
	return err
}

// maxSparseLength caps the length of sparse arrays, which can claim a large
// length with few elements.
const maxSparseLength = 1 << 20

func (d *Deserializer) newArray(dense bool) ([]any, error) {
	n, err := d.ReadVarint()
	if err != nil {
		return nil, err
	}
	// Every element of a dense array takes at least one byte.
	if dense && n > uint64(len(d.data)-d.pos) {
		return nil, fmt.Errorf("%w: dense array of %d elements in %d bytes", ErrInvalid, n, len(d.data)-d.pos)
	}
	if !dense && n > maxSparseLength {
		return nil, fmt.Errorf("%w: sparse array length %d too large", ErrUnsupported, n)
	}
	if err := d.reserve(n); err != nil {
		return nil, err
	}
	arr := make([]any, n)
	d.objects = append(d.objects, arr)
	return arr, nil
}

func (d *Deserializer) readSparseArray() (any, error) {
	arr, err := d.newArray(false)
	if err != nil {
		return nil, err
	}
	if err := d.readArrayTail(arr, tagEndSparseArray); err != nil {
		return nil, err
	}
	return arr, nil
}

func (d *Deserializer) readDenseArray() (any, error) {
	arr, err := d.newArray(true)
	if err != nil {
		return nil, err
	}
	for i := range arr {
		v, err := d.readObject()
		if err != nil {
			return nil, err
		}
		arr[i] = v
	}
	if err := d.readArrayTail(arr, tagEndDenseArray); err != nil {
		return nil, err
	}
	return arr, nil
}

func (d *Deserializer) readJSMap() (any, error) {
	m := NewObject()
	d.objects = append(d.objects, m)
	if err := d.readProperties(tagEndJSMap, m.add); err != nil {
		return nil, err
	}
	count, err := d.ReadVarint()
	if err != nil {
		return nil, err
	}
	if count != uint64(m.Len()*2) {
		return nil, fmt.Errorf("%w: map has %d entries, header says %d items", ErrInvalid, m.Len(), count)
	}
	return m, nil
}

func (d *Deserializer) readJSSet() (any, error) {
	s := &Set{}
	d.objects = append(d.objects, s)
	for {
		switch d.peekTag() {
		case tagEndJSSet:
			d.readTag()
			count, err := d.ReadVarint()
			if err != nil {
				return nil, err
			}
			if count != uint64(len(s.Items)) {
				return nil, fmt.Errorf("%w: set has %d items, header says %d", ErrInvalid, len(s.Items), count)
			}
			return s, nil
		case tagEndOfData:
			return nil, fmt.Errorf("%w: data ends inside a set", ErrInvalid)
		}
		v, err := d.readObject()
		if err != nil {
			return nil, err
		}
		s.Items = append(s.Items, v)
	}
}

// Typed array element formats: size in bytes and decoder.
var viewFormats = map[byte]struct {
	size   int
	decode func([]byte) any
}{
	'b': {1, func(b []byte) any { return int64(int8(b[0])) }},
	'B': {1, func(b []byte) any { return int64(b[0]) }},
	'C': {1, func(b []byte) any { return int64(b[0]) }},
	'w': {2, func(b []byte) any { return int64(int16(binary.LittleEndian.Uint16(b))) }},
	'W': {2, func(b []byte) any { return int64(binary.LittleEndian.Uint16(b)) }},
	'd': {4, func(b []byte) any { return int64(int32(binary.LittleEndian.Uint32(b))) }},
	'D': {4, func(b []byte) any { return int64(binary.LittleEndian.Uint32(b)) }},
	'f': {4, func(b []byte) any { return float64(math.Float32frombits(binary.LittleEndian.Uint32(b))) }},
	'F': {8, func(b []byte) any { return math.Float64frombits(binary.LittleEndian.Uint64(b)) }},
	'q': {8, func(b []byte) any { return int64(binary.LittleEndian.Uint64(b)) }},
	'Q': {8, func(b []byte) any { return new(big.Int).SetUint64(binary.LittleEndian.Uint64(b)) }},
	'?': {1, func(b []byte) any { return int64(b[0]) }},
}

func (d *Deserializer) readArrayBufferView(buffer any) (any, error) {
	raw, ok := buffer.([]byte)
	if !ok {
		return nil, fmt.Errorf("%w: array buffer view after a %T", ErrInvalid, buffer)
	}
	var vals [3]uint64
	for i := range vals {
		v, err := d.ReadVarint()
		if err != nil {
			return nil, err
		}
		vals[i] = v
	}
	subtag, offset, length := vals[0], vals[1], vals[2]
	if d.version >= versionWithViewFlags {
		if _, err := d.ReadVarint(); err != nil {
			return nil, err
		}
	}
	if offset > uint64(len(raw)) || length > uint64(len(raw))-offset {
		return nil, fmt.Errorf("%w: array buffer view outside its buffer", ErrInvalid)
	}
	format, ok := viewFormats[byte(subtag)]
	if !ok || subtag > 0xff {
		return nil, fmt.Errorf("%w: array buffer view subtag %d", ErrUnsupported, subtag)
	}
	if length%uint64(format.size) != 0 {
		return nil, fmt.Errorf("%w: array buffer view length %d is not a multiple of %d", ErrInvalid, length, format.size)
	}
	data := raw[offset : offset+length]
	if err := d.reserve(uint64(len(data) / format.size)); err != nil {
		return nil, err
	}
	items := make([]any, 0, len(data)/format.size)
	for i := 0; i < len(data); i += format.size {
		items = append(items, format.decode(data[i:i+format.size]))
	}
	// V8 gives the view its own object id.
	d.objects = append(d.objects, items)
	return items, nil
}
