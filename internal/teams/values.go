package teams

import (
	"fmt"
	"html"
	"math"
	"math/big"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/stn1slv/teamscache/internal/v8"
)

// The helpers below turn decoded values into the text of the output fields.
// The rules are fixed, so that the output stays stable: null is "None",
// undefined is "<Undefined>", booleans are "True" and "False", and a value is
// "empty" when it is null, undefined, false, zero, or an empty string, list or
// object.

// valueString formats a decoded value as text.
func valueString(v any) string {
	switch x := v.(type) {
	case nil:
		return "None"
	case v8.Undefined:
		return "<Undefined>"
	case bool:
		if x {
			return "True"
		}
		return "False"
	case int64:
		return strconv.FormatInt(x, 10)
	case *big.Int:
		return x.String()
	case float64:
		return floatString(x)
	case string:
		return x
	case v8.Date:
		return dateString(x)
	default:
		// Objects, arrays and other values are not printed: they can refer
		// to themselves, and a field holding one is not text anyway.
		return fmt.Sprintf("<%T>", v)
	}
}

// floatString formats a float with the shortest digits that round-trip, in
// fixed notation for exponents from -4 to 15 (always with a decimal point,
// like "1768557031836.0"), otherwise in scientific notation (like "1e+16").
func floatString(f float64) string {
	switch {
	case math.IsNaN(f):
		return "nan"
	case math.IsInf(f, 1):
		return "inf"
	case math.IsInf(f, -1):
		return "-inf"
	}
	s := strconv.FormatFloat(f, 'e', -1, 64) // like -1.768557031836e+12
	sign := ""
	if s[0] == '-' {
		sign, s = "-", s[1:]
	}
	mantissa, expText, _ := strings.Cut(s, "e")
	exp, _ := strconv.Atoi(expText)
	digits := strings.Replace(mantissa, ".", "", 1)

	if exp < -4 || exp >= 16 {
		out := digits[:1]
		if len(digits) > 1 {
			out += "." + digits[1:]
		}
		expSign := "+"
		if exp < 0 {
			expSign, exp = "-", -exp
		}
		return fmt.Sprintf("%s%se%s%02d", sign, out, expSign, exp)
	}
	point := exp + 1
	switch {
	case point <= 0:
		return sign + "0." + strings.Repeat("0", -point) + digits
	case point >= len(digits):
		return sign + digits + strings.Repeat("0", point-len(digits)) + ".0"
	default:
		return sign + digits[:point] + "." + digits[point:]
	}
}

// dateString formats a date in UTC as "2006-01-02 15:04:05+00:00", with
// ".000000" microseconds only when they are not zero.
func dateString(d v8.Date) string {
	micros := int64(math.RoundToEven(d.Millis * 1000))
	t := time.UnixMicro(micros).UTC()
	out := t.Format("2006-01-02 15:04:05")
	if us := t.Nanosecond() / 1000; us != 0 {
		out += fmt.Sprintf(".%06d", us)
	}
	return out + "+00:00"
}

// truthy reports whether a decoded value is not empty.
func truthy(v any) bool {
	switch x := v.(type) {
	case nil, v8.Undefined:
		return false
	case bool:
		return x
	case int64:
		return x != 0
	case *big.Int:
		return x.Sign() != 0
	case float64:
		return x != 0
	case string:
		return x != ""
	case []byte:
		return len(x) > 0
	case []any:
		return len(x) > 0
	case *v8.Object:
		return x.Len() > 0
	case *v8.Set:
		return len(x.Items) > 0
	default:
		return true
	}
}

// or returns a if it is truthy, else b.
func or(a, b any) any {
	if truthy(a) {
		return a
	}
	return b
}

// get returns obj[key], or nil when obj is not an object or has no such key.
func get(obj any, key string) any {
	o, ok := obj.(*v8.Object)
	if !ok {
		return nil
	}
	v, _ := o.Get(key)
	return v
}

// getOr returns obj[key], or def when the key is missing, like dict.get(key, def).
func getOr(o *v8.Object, key string, def any) any {
	if v, ok := o.Get(key); ok {
		return v
	}
	return def
}

var (
	tagRE = regexp.MustCompile(`<[^>]+>`)
	// Line breaks and the ends of block elements separate lines of text.
	breakRE = regexp.MustCompile(`(?i)<br\s*/?>|</(?:p|div|li|tr|h[1-6]|blockquote|pre)\s*>`)
)

// plainText returns a plain field without interpreting markup or entities.
func plainText(v any) string {
	var s string
	switch x := v.(type) {
	case nil, v8.Undefined:
		return ""
	case string:
		s = x
	case []byte:
		s = strings.ToValidUTF8(string(x), "�")
	default:
		s = valueString(v)
	}
	return stripSpace(s)
}

// text converts an HTML field to plain text.
func text(v any) string {
	s := plainText(v)
	// Remove tags before decoding entities, so that escaped text such as
	// "&lt;tag&gt;" stays in the result.
	s = tagRE.ReplaceAllString(breakRE.ReplaceAllString(s, "\n"), "")
	return stripSpace(html.UnescapeString(s))
}

// messageText preserves plain messages. Missing types keep the legacy HTML
// conversion because older cache records may omit messageType.
func messageText(message *v8.Object) string {
	kind := strings.ToLower(plainText(get(message, "messageType")))
	content := get(message, "content")
	if kind == "text" {
		return plainText(content)
	}
	return text(content)
}

// stripSpace removes leading and trailing whitespace: Unicode spaces plus
// the separators U+001C to U+001F.
func stripSpace(s string) string {
	return strings.TrimFunc(s, func(r rune) bool {
		return unicode.IsSpace(r) || (r >= 0x1c && r <= 0x1f)
	})
}
