package service

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stn1slv/teamscache/internal/idbtest"
	"github.com/stn1slv/teamscache/internal/teams"
)

var now = time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)

func opts() Options {
	return Options{MaxContent: 5, Now: func() time.Time { return now }}
}

// ms returns the epoch milliseconds of a time days days before now, as text.
func ms(daysAgo float64) string {
	return strconv.FormatInt(now.Add(-time.Duration(daysAgo*24*float64(time.Hour))).UnixMilli(), 10) + ".0"
}

func msg(account, conv, id, sender, ts, content string) teams.Message {
	return teams.Message{Account: account, ConversationID: conv, MessageID: id, Sender: sender, Timestamp: ts, Content: content}
}

func TestTitledRowsLeadToReadConversation(t *testing.T) {
	t.Parallel()
	snap := &Snapshot{
		Messages:      []teams.Message{msg("a", "c", "m", "Ann Lee", ms(0), "hi")},
		Conversations: []teams.Conversation{{Account: "a", ID: "c", Title: "Ann Lee"}},
		Mentions:      []teams.Mention{{Account: "a", ConversationID: "c", MessageID: "m", Timestamp: ms(0)}},
	}

	overview, err := snap.Overview(context.Background(), 7, "", opts())
	require.NoError(t, err)
	mentions, err := snap.MentionsQuery(context.Background(), 0, false, "", 100, opts())
	require.NoError(t, err)

	require.Len(t, overview, 1)
	require.Len(t, mentions, 1)
	assert.Equal(t, "Ann Lee", overview[0].Conversation)
	assert.Equal(t, "Ann Lee", mentions[0].Conversation)
	got, err := snap.ReadConversation(context.Background(), overview[0].ConversationID, "", 50)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "hi", got[0].Content)
}

func TestQueries(t *testing.T) {
	t.Parallel()
	snap := &Snapshot{
		Messages: []teams.Message{
			msg("a", "c1", "1", "Ann (Org)", ms(1), "Budget plan"),
			msg("a", "c1", "2", "Bob", ms(3), "older budget"),
			msg("a", "c2", "3", "Cid", ms(10), "very old"),
			msg("b", "c3", "4", "Dan", ms(0.5), "other account"),
			msg("a", "c1", "5", "Eve", "", "no time"),
		},
		Conversations: []teams.Conversation{
			{Account: "a", ID: "c1", Title: "Plan"},
			{Account: "a", ID: "c2"},
			{Account: "b", ID: "c3", LastMessage: "x"},
		},
		Mentions: []teams.Mention{
			{Account: "a", ConversationID: "c1", MessageID: "1", Timestamp: ms(1), IsRead: false},
			{Account: "a", ConversationID: "c2", MessageID: "3", Timestamp: ms(10), IsRead: true},
		},
	}

	t.Run("search ignores case and filters by days", func(t *testing.T) {
		t.Parallel()
		for _, tc := range []struct {
			query              string
			days, limit, count int
		}{{"BUDGET", 0, 0, 2}, {"budget", 2, 0, 1}, {"budget", 0, 1, 1}, {"org", 0, 0, 1}} {
			got, err := snap.Search(context.Background(), tc.query, "", tc.days, tc.limit, opts())
			require.NoError(t, err)
			assert.Len(t, got, tc.count, tc.query)
		}
	})
	t.Run("recent is newest first and skips messages without time", func(t *testing.T) {
		t.Parallel()
		got, err := snap.Recent(context.Background(), 7, "", 0, opts())
		require.NoError(t, err)
		require.Len(t, got, 3)
		assert.Equal(t, []string{"4", "1", "2"}, []string{got[0].MessageID, got[1].MessageID, got[2].MessageID})
		got, err = snap.Recent(context.Background(), 7, "a", 0, opts())
		require.NoError(t, err)
		assert.Len(t, got, 2)
		got, err = snap.Recent(context.Background(), 0, "", 0, opts())
		require.NoError(t, err)
		assert.Len(t, got, 5, "0 days means no cutoff, undated messages included")
	})
	t.Run("read conversation keeps the newest, oldest first", func(t *testing.T) {
		t.Parallel()
		got, err := snap.ReadConversation(context.Background(), "c1", "", 2)
		require.NoError(t, err)
		require.Len(t, got, 2)
		assert.Equal(t, "2", got[0].MessageID)
		assert.Equal(t, "1", got[1].MessageID)
	})
	t.Run("conversations need a title or a last message", func(t *testing.T) {
		t.Parallel()
		got, err := snap.ListConversations(context.Background(), "", 0)
		require.NoError(t, err)
		assert.Len(t, got, 2)
		got, err = snap.ListConversations(context.Background(), "b", 0)
		require.NoError(t, err)
		assert.Len(t, got, 1)
	})
	t.Run("mentions", func(t *testing.T) {
		t.Parallel()
		all, err := snap.MentionsQuery(context.Background(), 0, false, "", 0, opts())
		require.NoError(t, err)
		require.Len(t, all, 2)
		assert.Equal(t, "Plan", all[0].Conversation)
		assert.Equal(t, "Budge…", all[0].Content, "content is cut to MaxContent characters")
		assert.Equal(t, "c2", all[1].Conversation, "without a title, the id is shown")
		all, err = snap.MentionsQuery(context.Background(), 0, true, "", 0, opts())
		require.NoError(t, err)
		assert.Len(t, all, 1)
		all, err = snap.MentionsQuery(context.Background(), 5, false, "", 0, opts())
		require.NoError(t, err)
		assert.Len(t, all, 1)
	})
	t.Run("overview groups by conversation", func(t *testing.T) {
		t.Parallel()
		got, err := snap.Overview(context.Background(), 7, "", opts())
		require.NoError(t, err)
		require.Len(t, got, 2)
		assert.Equal(t, "c3", got[0].ConversationID)
		assert.Equal(t, OverviewRow{
			Conversation: "Plan", ConversationID: "c1", Count: 2, Senders: []string{"Ann (Org)", "Bob"},
			LastSender: "Ann (Org)", LastMessage: "Budge…", LastTime: ms(1), Account: "a",
		}, got[1], "the last message is cut to MaxContent characters")
		got, err = snap.Overview(context.Background(), 0, "", opts())
		require.NoError(t, err)
		assert.Len(t, got, 3, "0 days means no cutoff")
	})
}

func TestListConversationsSortsNewestFirstBeforeLimit(t *testing.T) {
	t.Parallel()
	snap := &Snapshot{Conversations: []teams.Conversation{
		{Account: "a", ID: "old", Title: "Old", LastMessageTime: ms(10)},
		{Account: "a", ID: "undated", Title: "Undated"},
		{Account: "a", ID: "new", Title: "New", LastMessageTime: ms(1)},
		{Account: "a", ID: "mid", Title: "Mid", LastMessageTime: ms(3)},
	}}

	got, err := snap.ListConversations(context.Background(), "", 2)

	require.NoError(t, err)
	require.Len(t, got, 2)
	assert.Equal(t, []string{"new", "mid"}, []string{got[0].ID, got[1].ID})

	all, err := snap.ListConversations(context.Background(), "", 0)
	require.NoError(t, err)
	require.Len(t, all, 4)
	assert.Equal(t, "undated", all[3].ID)
}

func TestTruncate(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "héllo", Truncate("héllo", 5))
	assert.Equal(t, "hé…", Truncate("héllo", 2))
	assert.Equal(t, "héllo", Truncate("héllo", 0), "0 means no limit")
	assert.Equal(t, "héllo", Truncate("héllo", -1), "a negative limit does not panic")
}

func TestOptionsWithoutNow(t *testing.T) {
	t.Parallel()
	ts := strconv.FormatInt(time.Now().UnixMilli(), 10)
	snap := &Snapshot{Messages: []teams.Message{msg("a", "c", "1", "Ann", ts, "hi")}}

	got, err := snap.Search(context.Background(), "hi", "", 1, 0, Options{})
	require.NoError(t, err)
	assert.Len(t, got, 1, "a nil Now means time.Now")
}

func TestMentionContentComesFromTheNewestVersion(t *testing.T) {
	t.Parallel()
	older := msg("a", "c", "m", "Ann", ms(0), "first text")
	older.Seq = 9
	newer := msg("a", "c", "m", "Ann", ms(0), "edited text")
	newer.Seq = 12
	elsewhere := msg("a", "other", "m", "Bob", ms(0), "same id in another chat")
	elsewhere.Seq = 20
	snap := &Snapshot{
		Messages: []teams.Message{newer, older, elsewhere},
		Mentions: []teams.Mention{{Account: "a", ConversationID: "c", MessageID: "m", Timestamp: ms(0)}},
	}

	got, err := snap.MentionsQuery(context.Background(), 0, false, "", 0, Options{})
	require.NoError(t, err)

	require.Len(t, got, 1)
	assert.Equal(t, "edited text", got[0].Content)
}

func TestEpochMillis(t *testing.T) {
	t.Parallel()
	tests := []struct {
		in   string
		want float64
		ok   bool
	}{
		{"1768557031836.0", 1768557031836, true},
		{"1768557031836", 1768557031836, true},
		{"2026-01-16T09:50:31.836Z", 1768557031836, true},
		{"2026-01-16T09:50:31+00:00", 1768557031000, true},
		{"2026-01-16 09:50:31.836000+00:00", 1768557031836, true}, // how dates are printed
		{"  2026-01-16T09:50:31.836Z  ", 1768557031836, true},
		{"", 0, false},
		{"soon", 0, false},
	}
	for _, tt := range tests {
		got, ok := EpochMillis(tt.in)
		assert.Equal(t, tt.ok, ok, tt.in)
		assert.InDelta(t, tt.want, got, 0.001, tt.in)
	}
}

func TestAccountLabels(t *testing.T) {
	t.Parallel()
	accounts := []teams.Account{{TenantID: "t", UserID: "u"}, {TenantID: "t", UserID: "u"}, {TenantID: "x", UserID: "y"}}
	messages := []teams.Message{
		{Account: "t:u", Sender: "Ann (B Org)"},
		{Account: "t:u", Sender: "Bob (A Org)"},
		{Account: "t:u", Sender: "Cid (A Org)"},
		{Account: "t:u", Sender: "Dan (B Org)"},
		{Account: "t:u", Sender: "Eve"},
	}

	got := accountsWithLabels(accounts, messages)

	assert.Equal(t, []Account{
		{Key: "t:u", TenantID: "t", UserID: "u", Label: "B Org"},
		{Key: "x:y", TenantID: "x", UserID: "y", Label: ""},
	}, got, "a tie goes to the label seen first, and repeated contexts are listed once")
}

func TestAccountsPreferProfilesOverMessageSuffixes(t *testing.T) {
	t.Parallel()
	accounts := []teams.Account{{TenantID: "t1", UserID: "u1"}, {TenantID: "t2", UserID: "u2"}}
	profiles := []teams.Profile{
		{Account: "t1:u1", TenantName: "Contoso", DisplayName: "Ann Lee", Email: "ann@contoso.test"},
		{Account: "t2:u2", DisplayName: "Bob"}, // empty TenantName falls back to OrgLabel
	}
	messages := []teams.Message{
		{Account: "t1:u1", Sender: "Guest (Other Org)"},
		{Account: "t2:u2", Sender: "Bob (Fabrikam)"},
	}

	got := accountsWithProfiles(accounts, profiles, messages)

	assert.Equal(t, []Account{
		{Key: "t1:u1", TenantID: "t1", UserID: "u1", Label: "Contoso", TenantName: "Contoso", DisplayName: "Ann Lee", Email: "ann@contoso.test"},
		{Key: "t2:u2", TenantID: "t2", UserID: "u2", Label: "Fabrikam", DisplayName: "Bob"},
	}, got)
}

func TestLoadForAccountsUsesProfilesWithoutDecodingMessages(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	dir, blobs := filepath.Join(root, "cache.leveldb"), filepath.Join(root, "cache.blob")
	p := func(k string, v any) idbtest.Prop { return idbtest.Prop{Key: k, Value: v} }
	require.NoError(t, idbtest.Write(dir, blobs, []idbtest.Database{
		{
			Number: 1, Name: "Teams:replychain-manager:react-web-client:t:u:en-us",
			Stores: []idbtest.Store{{Name: "replychains", Records: []idbtest.Record{
				{Key: "bad", Value: "x", Wrapping: idbtest.Corrupt},
				{Key: "blob", Value: idbtest.Obj{p("messageMap", idbtest.Obj{p("m", idbtest.Obj{p("content", "msg")})})}, Wrapping: idbtest.Blob},
			}}},
		},
		{
			Number: 2, Name: "Teams:profiles:react-web-client:t:u:en-us",
			Stores: []idbtest.Store{{Name: "profiles", Records: []idbtest.Record{
				{Key: "u", Value: idbtest.Obj{p("objectId", "u"), p("tenantName", "Contoso"), p("displayName", "Ann"), p("email", "ann@contoso.test")}},
			}}},
		},
		{
			Number: 3, Name: "Teams:conversation-manager:react-web-client:t:u:en-us",
			Stores: []idbtest.Store{{Name: "conversations", Records: []idbtest.Record{
				{Key: "c1", Value: idbtest.Obj{p("id", "c1"), p("threadProperties", idbtest.Obj{p("topic", "Project")})}},
			}}},
		},
	}))
	require.NoError(t, os.Remove(filepath.Join(blobs, "1", "00", "1")))

	snap, err := LoadFor(context.Background(), dir, slog.New(slog.DiscardHandler), teams.ScopeProfiles)
	require.NoError(t, err)
	assert.Zero(t, snap.Skipped, "replychains is not decoded when profiles provides TenantName")
	assert.Empty(t, snap.Damage, "missing message blobs are not copied when profiles provides TenantName")
	assert.Empty(t, snap.Messages)
	assert.Equal(t, []Account{{
		Key: "t:u", TenantID: "t", UserID: "u", Label: "Contoso",
		TenantName: "Contoso", DisplayName: "Ann", Email: "ann@contoso.test",
	}}, snap.Accounts)

	convSnap, err := LoadFor(context.Background(), dir, slog.New(slog.DiscardHandler), teams.ScopeConversations)
	require.NoError(t, err)
	assert.Zero(t, convSnap.Skipped, "conversations scope does not decode messages")
	assert.Empty(t, convSnap.Damage, "conversations scope does not copy message blobs")
	assert.Empty(t, convSnap.Messages)
	require.Len(t, convSnap.Conversations, 1)
}

func TestDumpAndMessageRowIdentityFields(t *testing.T) {
	t.Parallel()
	sentTrue := true
	snap := &Snapshot{
		Accounts:      []Account{{Key: "a", TenantID: "t", UserID: "u", Label: "Org", TenantName: "Org"}},
		Conversations: []teams.Conversation{{Account: "a", ID: "c1", Title: "Project", LastMessage: "long preview"}},
		Messages: []teams.Message{
			{Account: "a", ConversationID: "c1", MessageID: "m1", Sender: "Ann", Creator: "8:orgid:u", IsSentByCurrentUser: &sentTrue, Timestamp: ms(1), Content: "hello world"},
			{Account: "b", ConversationID: "c2", MessageID: "m2", Sender: "Bob", Timestamp: ms(1), Content: "other tenant"},
		},
		Mentions: []teams.Mention{{Account: "a", ConversationID: "c1", MessageID: "m1", Timestamp: ms(1), IsRead: false}},
	}

	got, err := snap.Dump(context.Background(), 7, "a", opts())
	require.NoError(t, err)
	require.Len(t, got.Accounts, 1)
	require.Len(t, got.Conversations, 1)
	assert.Equal(t, "long …", got.Conversations[0].LastMessage)
	require.Len(t, got.Messages, 1)
	assert.Equal(t, "hello…", got.Messages[0].Content)
	assert.Equal(t, "8:orgid:u", got.Messages[0].Creator)
	require.NotNil(t, got.Messages[0].IsSentByCurrentUser)
	assert.True(t, *got.Messages[0].IsSentByCurrentUser)
	require.Len(t, got.Mentions, 1)
	assert.False(t, got.Partial)
	assert.NotNil(t, got.Damage)

	rows, err := Rows(context.Background(), got.Messages, 0)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, "8:orgid:u", rows[0].Creator)
	require.NotNil(t, rows[0].IsSentByCurrentUser)
	assert.True(t, *rows[0].IsSentByCurrentUser)
}

func TestCacheParsesAgainWhenTheFolderChanges(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "cache.leveldb")
	p := func(k string, v any) idbtest.Prop { return idbtest.Prop{Key: k, Value: v} }
	require.NoError(t, idbtest.Write(dir, "", []idbtest.Database{{
		Number: 1, Name: "Teams:replychain-manager:react-web-client:t:u:en-us",
		Stores: []idbtest.Store{{Name: "replychains", Records: []idbtest.Record{
			{Key: "r", Value: idbtest.Obj{p("messageMap", idbtest.Obj{p("m", idbtest.Obj{p("content", "hi")})})}},
		}}},
	}}))
	cache := NewCache(dir, time.Hour, slog.New(slog.DiscardHandler))

	first, err := cache.Snapshot(context.Background())
	require.NoError(t, err)
	again, err := cache.Snapshot(context.Background())
	require.NoError(t, err)
	assert.Same(t, first, again, "an unchanged folder is not parsed again")

	require.NoError(t, os.WriteFile(filepath.Join(dir, "extra"), []byte("changed"), 0o600))
	changed, err := cache.Snapshot(context.Background())
	require.NoError(t, err)
	assert.NotSame(t, first, changed)
	assert.Equal(t, "hi", changed.Messages[0].Content)
	assert.Equal(t, []Account{{Key: "t:u", TenantID: "t", UserID: "u"}}, changed.Accounts)
}

func TestCacheHonorsCancellation(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "cache.leveldb")
	require.NoError(t, idbtest.WriteTeamsSample(dir, time.Now()))
	cache := NewCache(dir, time.Hour, slog.New(slog.DiscardHandler))
	_, err := cache.Snapshot(context.Background())
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	snap, err := cache.Snapshot(ctx)
	require.ErrorIs(t, err, context.Canceled)
	assert.Nil(t, snap, "an already canceled request does not receive a cached snapshot")

	cache.gate <- struct{}{}
	t.Cleanup(func() { <-cache.gate })
	ctx, cancel = context.WithTimeout(context.Background(), 20*time.Millisecond)
	t.Cleanup(cancel)
	snap, err = cache.Snapshot(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Nil(t, snap, "waiting for another load respects the request deadline")
}

func TestLoadReportsDamagedSections(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "cache.leveldb")
	require.NoError(t, idbtest.WriteTeamsSample(dir, time.Now()))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "999999.ldb"), []byte("corrupt"), 0o600))

	snap, err := Load(context.Background(), dir, slog.New(slog.DiscardHandler))

	require.NoError(t, err)
	require.Len(t, snap.Damage, 1)
	assert.Equal(t, "999999.ldb", snap.Damage[0].File)
	assert.Zero(t, snap.Skipped, "file damage is counted separately from undecodable records")
	assert.Len(t, snap.Messages, 2)
}

func TestOverviewPrefersTheNewerVersionOnEqualTimes(t *testing.T) {
	t.Parallel()
	ts := ms(1)
	older := msg("a", "c", "m", "Ann", ts, "before the edit")
	older.Seq = 3
	newer := msg("a", "c", "m", "Ann", ts, "after the edit")
	newer.Seq = 8
	undated := msg("a", "d", "x", "Bob", "", "no time")
	snap := &Snapshot{Messages: []teams.Message{older, newer, undated}}

	got, err := snap.Overview(context.Background(), 0, "", Options{})
	require.NoError(t, err)

	require.Len(t, got, 2)
	assert.Equal(t, "after the edit", got[0].LastMessage)
	assert.Equal(t, "no time", got[1].LastMessage, "an undated conversation still has a last message and comes last")
}

func TestSearchReturnsTheNewestMatchesFirst(t *testing.T) {
	t.Parallel()
	snap := &Snapshot{Messages: []teams.Message{
		msg("a", "c", "old", "Ann", ms(30), "budget"),
		msg("a", "c", "none", "Ann", "", "budget"),
		msg("a", "c", "new", "Ann", ms(1), "budget"),
	}}

	got, err := snap.Search(context.Background(), "budget", "", 0, 2, opts())
	require.NoError(t, err)

	require.Len(t, got, 2)
	assert.Equal(t, []string{"new", "old"}, []string{got[0].MessageID, got[1].MessageID})
}

func TestEpochMillisRejectsNonFiniteValues(t *testing.T) {
	t.Parallel()
	for _, ts := range []string{"nan", "inf", "-inf", "NaN"} {
		_, ok := EpochMillis(ts)
		assert.False(t, ok, ts)
	}
}

func TestQueriesStopAfterSnapshotLoadIsCanceled(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "cache.leveldb")
	require.NoError(t, idbtest.WriteTeamsSample(dir, time.Now()))
	cache := NewCache(dir, time.Hour, slog.New(slog.DiscardHandler))
	snap, err := cache.Snapshot(context.Background())
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, query := range []func() error{
		func() error { _, err := snap.Search(ctx, "budget", "", 0, 0, Options{}); return err },
		func() error { _, err := snap.Recent(ctx, 0, "", 0, Options{}); return err },
		func() error { _, err := snap.ReadConversation(ctx, "c1", "", 0); return err },
		func() error { _, err := snap.ListConversations(ctx, "", 0); return err },
		func() error { _, err := snap.MentionsQuery(ctx, 0, false, "", 0, Options{}); return err },
		func() error { _, err := snap.Overview(ctx, 0, "", Options{}); return err },
		func() error { _, err := Rows(ctx, snap.Messages, 600); return err },
	} {
		require.ErrorIs(t, query(), context.Canceled)
	}
	ctx, cancel = context.WithCancel(context.Background())
	t.Cleanup(cancel)
	options := Options{Now: func() time.Time { cancel(); return time.Now() }}
	rows, err := snap.Search(ctx, "budget", "", 7, 0, options)
	require.ErrorIs(t, err, context.Canceled, "cancellation after the query starts stops the scan")
	assert.Nil(t, rows)
}

func TestSortContextPreservesOrderAndStopsDuringSorting(t *testing.T) {
	t.Parallel()
	type item struct{ key, id int }
	items := []item{{2, 1}, {1, 2}, {1, 3}, {3, 4}, {2, 5}}
	require.NoError(t, sortContext(context.Background(), items, func(a, b item) bool { return a.key < b.key }))
	assert.Equal(t, []item{{1, 2}, {1, 3}, {2, 1}, {2, 5}, {3, 4}}, items)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	comparisons := 0
	err := sortContext(ctx, []int{5, 4, 3, 2, 1}, func(a, b int) bool {
		comparisons++
		cancel()
		return a < b
	})
	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, 1, comparisons)
}
