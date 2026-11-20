// Package v8 decodes values written by the V8 ValueSerializer, as used by
// Chromium for IndexedDB, including the Blink host objects.
//
// Ported from ccl_v8_value_deserializer.py and ccl_blink_value_deserializer.py
// in ccl_chromium_reader (MIT, CCL Forensics).
// Format reference: https://github.com/v8/v8/blob/main/src/objects/value-serializer.cc
//
// Decoded values use these Go types:
//
//	null                     nil
//	undefined, the hole      Undefined
//	true, false (and objects) bool
//	Int32, Uint32            int64
//	Double, Number object    float64
//	BigInt                   *big.Int
//	strings                  string
//	Date                     Date
//	RegExp                   RegExp
//	Object, Map              *Object (keys keep their order)
//	Array                    []any
//	Set                      *Set
//	ArrayBuffer              []byte
//	typed array              []any of numbers
//	Blink host objects       BlobIndex, []BlobIndex, NativeFileHandle, CryptoKey
package v8

import (
	"math"
	"math/big"
	"strconv"
	"time"
)

// Undefined is the JavaScript undefined value.
type Undefined struct{}

// Date is a JavaScript Date, in milliseconds since the Unix epoch.
type Date struct {
	Millis float64
}

// Time returns the date as a UTC time.
func (d Date) Time() time.Time {
	sec, frac := math.Modf(d.Millis / 1000)
	return time.Unix(int64(sec), int64(frac*1e9)).UTC()
}

// RegExp is a JavaScript regular expression.
type RegExp struct {
	Pattern string
	Flags   uint64
}

// Set is a JavaScript Set.
type Set struct {
	Items []any
}

// Object is a JavaScript object or Map. Keys keep the order they were read in.
// Keys compare as in a JavaScript Map: strings, numbers, booleans and BigInts
// by value (1 and 1.0 are the same key), other values by identity. A repeated
// key replaces the earlier value in place.
type Object struct {
	keys    []any
	values  []any
	index   map[string]int
	objects int // count of keys compared by identity
}

// NewObject returns an empty object.
func NewObject() *Object {
	return &Object{index: map[string]int{}}
}

// Set adds or replaces a property.
func (o *Object) Set(key, value any) {
	id := o.keyID(key)
	if i, ok := o.index[id]; ok {
		o.values[i] = value
		return
	}
	o.index[id] = len(o.keys)
	o.keys = append(o.keys, key)
	o.values = append(o.values, value)
}

// add is Set in the form readProperties takes.
func (o *Object) add(key, value any) error {
	o.Set(key, value)
	return nil
}

// Get returns the value of a string key.
func (o *Object) Get(key string) (any, bool) {
	i, ok := o.index["s"+key]
	if !ok {
		return nil, false
	}
	return o.values[i], true
}

// Len returns the number of properties.
func (o *Object) Len() int { return len(o.keys) }

// Keys returns the keys in order.
func (o *Object) Keys() []any { return o.keys }

// Values returns the values in key order.
func (o *Object) Values() []any { return o.values }

// keyID maps a key to a string that is the same for keys that count as the
// same key. Objects, arrays and other non-primitive keys are compared by
// identity, so each one gets its own id; a valid Map never repeats them.
func (o *Object) keyID(key any) string {
	switch k := key.(type) {
	case string:
		return "s" + k
	case nil:
		return "n"
	case Undefined:
		return "u"
	case bool:
		if k {
			return "b1"
		}
		return "b0"
	case int64:
		return "i" + strconv.FormatInt(k, 10)
	case float64:
		if k == math.Trunc(k) && math.Abs(k) < 1<<63 {
			return "i" + strconv.FormatInt(int64(k), 10)
		}
		return "f" + strconv.FormatFloat(k, 'g', -1, 64)
	case *big.Int:
		return "z" + k.String()
	default:
		o.objects++
		return "x" + strconv.Itoa(o.objects)
	}
}
