package mcpserver

import (
	"context"
	"encoding/json"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stn1slv/teamscache/internal/idbtest"
	"github.com/stn1slv/teamscache/internal/service"
)

func connect(t *testing.T, account string) *mcp.ClientSession {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "sample.leveldb")
	require.NoError(t, idbtest.WriteTeamsSample(dir, time.Now()))
	cache := service.NewCache(dir, time.Minute, slog.New(slog.DiscardHandler))
	server := New(cache, service.Options{MaxContent: 6, Now: time.Now}, "test", account)

	ctx := context.Background()
	serverTransport, clientTransport := mcp.NewInMemoryTransports()
	serverSession, err := server.Connect(ctx, serverTransport, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = serverSession.Close() })
	client := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "test"}, nil)
	session, err := client.Connect(ctx, clientTransport, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })
	return session
}

func TestTools(t *testing.T) {
	t.Parallel()
	session := connect(t, "")

	res, err := session.ListTools(context.Background(), nil)
	require.NoError(t, err)

	names := map[string]bool{}
	for _, tool := range res.Tools {
		names[tool.Name] = true
		assert.True(t, tool.Annotations.ReadOnlyHint, tool.Name)
	}
	for _, name := range []string{
		"list_accounts", "list_conversations", "read_conversation", "search_messages",
		"recent_messages", "mentions", "unread_messages", "overview",
	} {
		assert.True(t, names[name], name)
	}
	schema, err := json.Marshal(res.Tools[0].InputSchema)
	require.NoError(t, err)
	assert.Contains(t, string(schema), `"type":"object"`)
}

func callTool(t *testing.T, session *mcp.ClientSession, name string, args map[string]any) []map[string]any {
	t.Helper()
	res, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: name, Arguments: args})
	require.NoError(t, err)
	require.False(t, res.IsError, "%v", res.Content)
	raw, err := json.Marshal(res.StructuredContent)
	require.NoError(t, err)
	var out struct {
		Result []map[string]any `json:"result"`
	}
	require.NoError(t, json.Unmarshal(raw, &out))
	return out.Result
}

func TestToolResults(t *testing.T) {
	t.Parallel()
	session := connect(t, "")

	accounts := callTool(t, session, "list_accounts", nil)
	require.Len(t, accounts, 1)
	assert.Equal(t, idbtest.SampleAccount, accounts[0]["key"])

	found := callTool(t, session, "search_messages", map[string]any{"query": "budget", "account": idbtest.SampleAccount})
	require.Len(t, found, 1)
	assert.Equal(t, "Budget…", found[0]["content"], "content is cut to the maximum length")
	assert.Equal(t, "c1", found[0]["conversation_id"])

	assert.Len(t, callTool(t, session, "recent_messages", map[string]any{"days": 7}), 2)
	assert.Len(t, callTool(t, session, "read_conversation", map[string]any{"conversation_id": "c1", "limit": 1}), 1)
	assert.Len(t, callTool(t, session, "unread_messages", nil), 1)
	assert.Equal(t, "Project", callTool(t, session, "overview", nil)[0]["conversation"])
}

func TestInvalidArgumentsAreRejected(t *testing.T) {
	t.Parallel()
	session := connect(t, "")

	res, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: "search_messages", Arguments: map[string]any{"days": 1}})

	assert.True(t, err != nil || res.IsError, "a call without the required query fails")
}

func TestServerBoundToOneAccount(t *testing.T) {
	t.Parallel()
	session := connect(t, idbtest.SampleAccount)

	assert.Len(t, callTool(t, session, "list_accounts", nil), 1)
	assert.Len(t, callTool(t, session, "recent_messages", map[string]any{"days": 7}), 2, "the bound account is used without an account argument")

	res, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name: "recent_messages", Arguments: map[string]any{"account": "other:account"},
	})
	require.NoError(t, err)
	assert.True(t, res.IsError, "another account is refused")
}

func TestServerBoundToAnotherAccountSeesNothing(t *testing.T) {
	t.Parallel()
	session := connect(t, "other:account")

	assert.Empty(t, callTool(t, session, "list_accounts", nil))
	assert.Empty(t, callTool(t, session, "recent_messages", map[string]any{"days": 7}))
}

func TestNegativeDaysAreRejected(t *testing.T) {
	t.Parallel()
	session := connect(t, "")

	res, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: "overview", Arguments: map[string]any{"days": -1}})

	require.NoError(t, err)
	assert.True(t, res.IsError)
}

func TestNegativeLimitIsRejected(t *testing.T) {
	t.Parallel()
	session := connect(t, "")

	res, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: "recent_messages", Arguments: map[string]any{"limit": -1}})

	require.NoError(t, err)
	assert.True(t, res.IsError)
}
