package teams

import (
	"math"
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/stn1slv/teamscache/internal/v8"
)

func TestFloatString(t *testing.T) {
	t.Parallel()
	tests := []struct {
		in   float64
		want string
	}{
		{1768557031836.0, "1768557031836.0"},
		{0.1, "0.1"},
		{math.Copysign(0, -1), "-0.0"},
		{1e16, "1e+16"},
		{1.5e-05, "1.5e-05"},
		{1e-4, "0.0001"},
		{123456789012345678.0, "1.2345678901234568e+17"},
		{2.5, "2.5"},
		{1e21, "1e+21"},
		{-3.14e-10, "-3.14e-10"},
		{9999999999999998.0, "9999999999999998.0"},
		{5e-324, "5e-324"},
		{1.7976931348623157e308, "1.7976931348623157e+308"},
		{math.NaN(), "nan"},
		{math.Inf(-1), "-inf"},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.want, floatString(tt.in), "floatString(%v)", tt.in)
	}
}

func TestValueString(t *testing.T) {
	t.Parallel()
	tests := []struct {
		in   any
		want string
	}{
		{nil, "None"},
		{v8.Undefined{}, "<Undefined>"},
		{true, "True"},
		{int64(-5), "-5"},
		{big.NewInt(7), "7"},
		{"text", "text"},
		{v8.Date{Millis: 0}, "1970-01-01 00:00:00+00:00"},
		{v8.Date{Millis: 1768557031836}, "2026-01-16 09:50:31.836000+00:00"},
		{v8.Date{Millis: 1768557031836.5}, "2026-01-16 09:50:31.836500+00:00"},
		{v8.Date{Millis: -1}, "1969-12-31 23:59:59.999000+00:00"},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.want, valueString(tt.in), "valueString(%#v)", tt.in)
	}
}

func TestTruthy(t *testing.T) {
	t.Parallel()
	falsy := []any{nil, v8.Undefined{}, false, int64(0), 0.0, "", []any{}, v8.NewObject(), &v8.Set{}, big.NewInt(0)}
	for _, v := range falsy {
		assert.False(t, truthy(v), "%#v", v)
	}
	truthyValues := []any{true, int64(1), math.NaN(), "x", []any{nil}, v8.Date{}, obj("a", nil)}
	for _, v := range truthyValues {
		assert.True(t, truthy(v), "%#v", v)
	}
}

func TestText(t *testing.T) {
	t.Parallel()
	tests := []struct {
		in   any
		want string
	}{
		{nil, ""},
		{"<div>Hello &amp; <b>you</b></div>", "Hello & you"},
		{"  spaced ", "spaced"},
		{"\x1ctext\x1f", "text"},
		{v8.Undefined{}, ""},
		{int64(12), "12"},
		{[]byte("raw"), "raw"},
		{"List&lt;String&gt; and a &lt; b", "List<String> and a < b"},
		{"<p>Hello</p><p>World</p>", "Hello\nWorld"},
		{"one<br>two<BR/>three", "one\ntwo\nthree"},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.want, text(tt.in), "text(%#v)", tt.in)
	}
}

func TestMessageTextPreservesPlainContent(t *testing.T) {
	t.Parallel()
	tests := []struct {
		kind    string
		content string
		want    string
	}{
		{"Text", "Keep x < 5 > 3 unchanged", "Keep x < 5 > 3 unchanged"},
		{"Text", "Use <placeholder> &amp; here", "Use <placeholder> &amp; here"},
		{"RichText/Html", "<p>Hello &amp; <b>you</b></p>", "Hello & you"},
		{"RichText/Media_GenericFile", "<URIObject><Title>Report</Title></URIObject>", "Report"},
		{"ThreadActivity/AddMember", "<addmember>Ann joined</addmember>", "Ann joined"},
		{"", "<p>Legacy</p>", "Legacy"},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.want, messageText(obj("messageType", tt.kind, "content", tt.content)))
	}
	assert.Equal(t, "Ann <guest> &amp; Co", plainText("Ann <guest> &amp; Co"))
}

func TestOrgLabel(t *testing.T) {
	t.Parallel()
	label, ok := OrgLabel("Ann Lee (GP Rubix)")
	assert.True(t, ok)
	assert.Equal(t, "GP Rubix", label)
	_, ok = OrgLabel("Ann Lee")
	assert.False(t, ok)
}
