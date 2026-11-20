package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stn1slv/teamscache/internal/idbtest"
)

func sampleCache(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "sample.leveldb")
	require.NoError(t, idbtest.WriteTeamsSample(dir, time.Now()))
	return dir
}

func runCLI(t *testing.T, args ...string) (stdout, stderr string, err error) {
	t.Helper()
	var out, errOut bytes.Buffer
	root := NewRootCommand("test", &out, &errOut)
	root.SetArgs(args)
	err = root.ExecuteContext(context.Background())
	return out.String(), errOut.String(), err
}

func TestCommands(t *testing.T) {
	t.Parallel()
	cache := sampleCache(t)
	tests := []struct {
		args      []string
		wantCount int
		wantFirst map[string]any
	}{
		{[]string{"accounts"}, 1, map[string]any{"key": idbtest.SampleAccount, "label": "Org"}},
		{[]string{"conversations"}, 1, map[string]any{"id": "c1", "title": "Project"}},
		{[]string{"search", "BUDGET"}, 1, map[string]any{"message_id": "m1", "content": "Budget <draft> is ready", "reply_chain_id": "r1", "content_type": "RichText/Html"}},
		{[]string{"search", "budget", "--account", "other:account"}, 0, nil},
		{[]string{"read", "c1"}, 2, map[string]any{"message_id": "m0"}},
		{[]string{"recent", "--days", "7"}, 2, map[string]any{"message_id": "m1"}},
		{[]string{"recent", "--days", "7", "--limit", "1"}, 1, nil},
		{[]string{"mentions", "--unread"}, 1, map[string]any{"conversation": "Project", "sender": "Ann Lee (Org)", "is_read": false}},
		{[]string{"overview"}, 1, map[string]any{"conversation": "Project", "count": 2.0}},
	}
	for _, tt := range tests {
		t.Run(tt.args[0], func(t *testing.T) {
			t.Parallel()
			stdout, stderr, err := runCLI(t, append(tt.args, "--leveldb", cache)...)
			require.NoError(t, err)

			var rows []map[string]any
			require.NoError(t, json.Unmarshal([]byte(stdout), &rows), stdout)
			require.Len(t, rows, tt.wantCount)
			for k, v := range tt.wantFirst {
				assert.Equal(t, v, rows[0][k], k)
			}
			assert.Contains(t, stderr, "record(s) skipped")
		})
	}
}

func TestOutputKeepsHTMLCharacters(t *testing.T) {
	t.Parallel()
	stdout, _, err := runCLI(t, "search", "budget", "--leveldb", sampleCache(t))
	require.NoError(t, err)
	assert.Contains(t, stdout, "Budget <draft> is ready")
	assert.NotContains(t, stdout, `\u003c`, "JSON output does not escape < as \\u003c")
}

func TestMissingCacheIsAnError(t *testing.T) {
	t.Parallel()
	_, _, err := runCLI(t, "accounts", "--leveldb", filepath.Join(t.TempDir(), "missing"))
	require.Error(t, err)
}

func TestNegativeValuesAreRejected(t *testing.T) {
	t.Parallel()
	cache := sampleCache(t)
	for _, args := range [][]string{
		{"mentions", "--max-content", "-1"},
		{"recent", "--days", "-1"},
		{"overview", "--days", "-3"},
		{"search", "x", "--limit", "-1"},
		{"mcp", "--cache-ttl", "-5s"},
	} {
		_, _, err := runCLI(t, append(args, "--leveldb", cache)...)
		require.ErrorIs(t, err, ErrInvalidFlag, args)
	}
}

func TestAccountsHonorsTheAccountFlag(t *testing.T) {
	t.Parallel()
	cache := sampleCache(t)

	stdout, _, err := runCLI(t, "accounts", "--account", "other:account", "--leveldb", cache)

	require.NoError(t, err)
	assert.JSONEq(t, "[]", stdout)
}
