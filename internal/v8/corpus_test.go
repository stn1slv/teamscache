package v8

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"math"
	"math/big"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// normalize turns a decoded value into the JSON shape that
// testdata/gen/golden.py writes for the same value decoded by ccl.
func normalize(t *testing.T, value any) any {
	t.Helper()
	switch v := value.(type) {
	case nil:
		return nil
	case Undefined:
		return map[string]any{"$undefined": true}
	case bool, string:
		return v
	case int64:
		return map[string]any{"$int": strconv.FormatInt(v, 10)}
	case *big.Int:
		return map[string]any{"$int": v.String()}
	case float64:
		return map[string]any{"$float": strconv.FormatUint(math.Float64bits(v), 10)}
	case []byte:
		return map[string]any{"$bytes": hex.EncodeToString(v)}
	case Date:
		// golden.py rounds to the nearest microsecond, ties to even.
		return map[string]any{"$date_us": strconv.FormatFloat(math.RoundToEven(v.Millis*1000), 'f', 0, 64)}
	case RegExp:
		return map[string]any{"$regexp": v.Pattern}
	case *Object:
		pairs := []any{}
		for i, k := range v.Keys() {
			pairs = append(pairs, []any{normalize(t, k), normalize(t, v.Values()[i])})
		}
		return map[string]any{"$dict": pairs}
	case []any:
		items := []any{}
		for _, item := range v {
			items = append(items, normalize(t, item))
		}
		return map[string]any{"$list": items}
	case *Set:
		items := []any{}
		for _, item := range v.Items {
			items = append(items, normalize(t, item))
		}
		sort.Slice(items, func(i, j int) bool { return sortKey(t, items[i]) < sortKey(t, items[j]) })
		return map[string]any{"$set": items}
	default:
		t.Fatalf("cannot normalize %T", value)
		return nil
	}
}

func sortKey(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return string(b)
}

// TestCorpusMatchesCCL decodes values written by Node's v8.serialize and
// compares them with what ccl_chromium_reader decoded from the same bytes.
func TestCorpusMatchesCCL(t *testing.T) {
	t.Parallel()
	bins, err := filepath.Glob(filepath.Join("testdata", "corpus", "*.bin"))
	require.NoError(t, err)
	require.NotEmpty(t, bins)

	for _, bin := range bins {
		name := strings.TrimSuffix(filepath.Base(bin), ".bin")
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			data, err := os.ReadFile(bin)
			require.NoError(t, err)
			golden, err := os.ReadFile(strings.TrimSuffix(bin, ".bin") + ".json")
			require.NoError(t, err)
			var want any
			require.NoError(t, json.Unmarshal(golden, &want))

			value, err := Deserialize(context.Background(), data, nil)

			if m, ok := want.(map[string]any); ok && m["$error"] == true {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			got, err := json.Marshal(normalize(t, value))
			require.NoError(t, err)
			wantJSON, err := json.Marshal(want)
			require.NoError(t, err)
			assert.JSONEq(t, string(wantJSON), string(got))
		})
	}
}

func mustHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(strings.ReplaceAll(s, " ", ""))
	require.NoError(t, err)
	return b
}

// TestKnownDifferencesFromCCL covers values where this package is
// deliberately correct and ccl is not. The bytes come from Node's
// v8.serialize.
func TestKnownDifferencesFromCCL(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		data string
		want any
	}{
		// ccl decodes ZigZag Int32 values off by one for negative numbers (-1 -> 0).
		{"negative int32", "ff0f 49 01", int64(-1)},
		{"negative int32 large", "ff0f 49 f3 01", int64(-122)},
		// ccl returns raw bytes for non-ASCII one-byte strings, which are Latin-1.
		{"latin-1 string", "ff0f 22 04 63 61 66 e9", "café"},
		// ccl fails on a lone surrogate; it becomes U+FFFD here.
		{"lone surrogate", "ff0f 63 02 00 d8", "�"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := Deserialize(context.Background(), mustHex(t, tt.data), nil)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestDeserializeErrors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		data string
	}{
		{"empty", ""},
		{"no version header", "49 02"},
		{"truncated string", "ff0f 22 05 61"},
		{"unknown tag", "ff0f 7e"},
		{"dangling reference", "ff0f 5e 03"},
		{"unterminated object", "ff0f 6f 22 01 61 49 02"},
		{"host object without reader", "ff0f 5c 69 00"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := Deserialize(context.Background(), mustHex(t, tt.data), nil)
			require.Error(t, err)
		})
	}
}

func TestBlinkHostObjects(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		data string
		want any
	}{
		{"blob index", "ff0f 5c 69 05", BlobIndex{Index: 5}},
		{"file index", "ff0f 5c 65 02", BlobIndex{IsFile: true, Index: 2}},
		{"file list", "ff0f 5c 4c 02 00 01", []BlobIndex{{IsFile: true, Index: 0}, {IsFile: true, Index: 1}}},
		{"file handle", "ff0f 5c 6e 03 61 62 63 07", NativeFileHandle{Name: "abc", TokenIndex: 7}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := Deserialize(context.Background(), mustHex(t, tt.data), ReadBlinkHostObject)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestObjectKeys(t *testing.T) {
	t.Parallel()
	o := NewObject()
	o.Set(int64(1), "int")
	o.Set(1.0, "float replaces int")
	o.Set("1", "string is a different key")
	o.Set(true, "boolean is a different key")
	o.Set(big.NewInt(1), "BigInt is a different key")
	o.Set(big.NewInt(1), "equal BigInt replaces it")
	o.Set(NewObject(), "objects are compared by identity")
	o.Set(NewObject(), "so a second object is a new key")

	assert.Equal(t, []any{
		"float replaces int", "string is a different key", "boolean is a different key",
		"equal BigInt replaces it", "objects are compared by identity", "so a second object is a new key",
	}, o.Values())
	v, ok := o.Get("1")
	assert.True(t, ok)
	assert.Equal(t, "string is a different key", v)
}

func TestArrayLimits(t *testing.T) {
	t.Parallel()
	// A dense array that claims 2^28 elements in a few bytes.
	_, err := Deserialize(context.Background(), mustHex(t, "ff0f 41 80808080 01"), nil)
	require.ErrorIs(t, err, ErrInvalid)
	// A sparse array longer than the cap.
	_, err = Deserialize(context.Background(), mustHex(t, "ff0f 61 80808080 01 40 00 00"), nil)
	require.ErrorIs(t, err, ErrUnsupported)
}

func TestMapWithBooleanAndNumberKeys(t *testing.T) {
	t.Parallel()
	// new Map([[true, "t"], [1, "one"]]) from Node's v8.serialize.
	got, err := Deserialize(context.Background(), mustHex(t, "ff0f 3b 54 22 01 74 49 02 22 03 6f 6e 65 3a 04"), nil)

	require.NoError(t, err)
	m, ok := got.(*Object)
	require.True(t, ok)
	assert.Equal(t, []any{true, int64(1)}, m.Keys())
}

func TestAllocationBudgetIsPerValue(t *testing.T) {
	t.Parallel()
	// A dense array of 64 empty sparse arrays, each claiming 2^20 slots.
	data := make([]byte, 0, 4+64*9+3)
	data = append(data, 0xff, 0x0f, 'A', 64)
	for range 64 {
		data = append(data, 'a', 0x80, 0x80, 0x40, '@', 0, 0x80, 0x80, 0x40)
	}
	data = append(data, '$', 0, 64)

	_, err := Deserialize(context.Background(), data, nil)

	require.ErrorIs(t, err, ErrUnsupported)
}

func TestCollectionPropertiesRespectAllocationBudget(t *testing.T) {
	t.Parallel()
	for _, data := range []string{"6f 22 01 6b 30 7b 01", "3b 22 01 6b 30 3a 02", "27 30 2c 01"} {
		d := &Deserializer{ctx: context.Background(), data: mustHex(t, data), version: 15, slots: maxSlots}
		_, err := d.readObject()
		require.ErrorIs(t, err, ErrUnsupported)
	}
}

func TestDeserializeStopsWhenCanceled(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := Deserialize(ctx, mustHex(t, "ff0f 30"), nil)
	require.ErrorIs(t, err, context.Canceled)

	ctx, cancel = context.WithCancel(context.Background())
	t.Cleanup(cancel)
	_, err = Deserialize(ctx, mustHex(t, "ff0f 5c 69 00"), func(*Deserializer) (any, error) {
		cancel()
		return nil, nil
	})
	require.ErrorIs(t, err, context.Canceled, "cancellation during decoding is not a bad-value error")
}
