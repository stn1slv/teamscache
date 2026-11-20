package v8

import (
	"fmt"
	"unicode/utf8"
)

// BlobIndex points to a Blob or File stored outside the value.
type BlobIndex struct {
	IsFile bool
	Index  uint64
}

// NativeFileHandle is a File System Access handle.
type NativeFileHandle struct {
	IsDir      bool
	Name       string
	TokenIndex uint64
}

// CryptoKey is a WebCrypto key.
type CryptoKey struct {
	SubType   byte
	Params    []uint64
	Usages    uint64
	KeyData   []byte
	PublicExp []byte
}

// Blink host object tags.
const (
	blinkBlobIndex        = 'i'
	blinkFileIndex        = 'e'
	blinkFileListIndex    = 'L'
	blinkNativeFileHandle = 'n'
	blinkNativeDirHandle  = 'N'
	blinkCryptoKey        = 'K'
	cryptoAesKey          = 1
	cryptoHmacKey         = 2
	cryptoRsaHashedKey    = 4
	cryptoEcKey           = 5
	cryptoNoParamsKey     = 6
	maxFileListLength     = 1 << 20
)

// ReadBlinkHostObject decodes the Blink host objects that IndexedDB values can
// hold (blob and file references, file handles, crypto keys). Other host
// objects return ErrUnsupported.
func ReadBlinkHostObject(d *Deserializer) (any, error) {
	tag, err := d.ReadByte()
	if err != nil {
		return nil, err
	}
	switch tag {
	case blinkBlobIndex, blinkFileIndex:
		i, err := d.ReadVarint()
		if err != nil {
			return nil, err
		}
		return BlobIndex{IsFile: tag == blinkFileIndex, Index: i}, nil
	case blinkFileListIndex:
		n, err := d.ReadVarint()
		if err != nil {
			return nil, err
		}
		// Each index takes at least one byte.
		if n > maxFileListLength || n > d.remaining() {
			return nil, fmt.Errorf("%w: file list length %d", ErrInvalid, n)
		}
		if err := d.reserve(n); err != nil {
			return nil, err
		}
		list := make([]BlobIndex, 0, n)
		for range n {
			i, err := d.ReadVarint()
			if err != nil {
				return nil, err
			}
			list = append(list, BlobIndex{IsFile: true, Index: i})
		}
		return list, nil
	case blinkNativeFileHandle, blinkNativeDirHandle:
		name, err := readBlinkString(d)
		if err != nil {
			return nil, err
		}
		token, err := d.ReadVarint()
		if err != nil {
			return nil, err
		}
		return NativeFileHandle{IsDir: tag == blinkNativeDirHandle, Name: name, TokenIndex: token}, nil
	case blinkCryptoKey:
		return readCryptoKey(d)
	default:
		return nil, fmt.Errorf("%w: blink host object tag %q", ErrUnsupported, rune(tag))
	}
}

func readBlinkString(d *Deserializer) (string, error) {
	n, err := d.ReadVarint32()
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

func readVarints(d *Deserializer, n int) ([]uint64, error) {
	out := make([]uint64, n)
	for i := range out {
		v, err := d.ReadVarint32()
		if err != nil {
			return nil, err
		}
		out[i] = v
	}
	return out, nil
}

func readCryptoKey(d *Deserializer) (any, error) {
	subType, err := d.ReadByte()
	if err != nil {
		return nil, err
	}
	key := CryptoKey{SubType: subType}
	switch subType {
	case cryptoAesKey, cryptoHmacKey:
		key.Params, err = readVarints(d, 2)
	case cryptoRsaHashedKey:
		// algorithm, key type (1 byte), modulus length, exponent, hash
		var params []uint64
		if params, err = readVarints(d, 1); err != nil {
			return nil, err
		}
		var keyType byte
		if keyType, err = d.ReadByte(); err != nil {
			return nil, err
		}
		var lengths []uint64
		if lengths, err = readVarints(d, 2); err != nil {
			return nil, err
		}
		if key.PublicExp, err = d.ReadRaw(lengths[1]); err != nil {
			return nil, err
		}
		var hash []uint64
		if hash, err = readVarints(d, 1); err != nil {
			return nil, err
		}
		key.Params = []uint64{params[0], uint64(keyType), lengths[0], hash[0]}
	case cryptoEcKey:
		var params []uint64
		if params, err = readVarints(d, 1); err != nil {
			return nil, err
		}
		var keyType byte
		if keyType, err = d.ReadByte(); err != nil {
			return nil, err
		}
		var curve []uint64
		if curve, err = readVarints(d, 1); err != nil {
			return nil, err
		}
		key.Params = []uint64{params[0], uint64(keyType), curve[0]}
	case cryptoNoParamsKey:
		key.Params, err = readVarints(d, 1)
	default:
		return nil, fmt.Errorf("%w: crypto key sub type %d", ErrUnsupported, subType)
	}
	if err != nil {
		return nil, err
	}
	tail, err := readVarints(d, 2)
	if err != nil {
		return nil, err
	}
	key.Usages = tail[0]
	if key.KeyData, err = d.ReadRaw(tail[1]); err != nil {
		return nil, err
	}
	return key, nil
}
