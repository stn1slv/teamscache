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

	overview := snap.Overview(7, "", opts())
	mentions := snap.MentionsQuery(0, false, "", 100, opts())

	require.Len(t, overview, 1)
	require.Len(t, mentions, 1)
	assert.Equal(t, "Ann Lee", overview[0].Conversation)
	assert.Equal(t, "Ann Lee", mentions[0].Conversation)
	got := snap.ReadConversation(overview[0].ConversationID, "", 50)
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
		assert.Len(t, snap.Search("BUDGET", "", 0, 0, opts()), 2)
		assert.Len(t, snap.Search("budget", "", 2, 0, opts()), 1)
		assert.Len(t, snap.Search("budget", "", 0, 1, opts()), 1)
		assert.Len(t, snap.Search("org", "", 0, 0, opts()), 1, "the sender is searched too")
	})
	t.Run("recent is newest first and skips messages without time", func(t *testing.T) {
		t.Parallel()
		got := snap.Recent(7, "", 0, opts())
		require.Len(t, got, 3)
		assert.Equal(t, []string{"4", "1", "2"}, []string{got[0].MessageID, got[1].MessageID, got[2].MessageID})
		assert.Len(t, snap.Recent(7, "a", 0, opts()), 2)
		assert.Len(t, snap.Recent(0, "", 0, opts()), 5, "0 days means no cutoff, undated messages included")
	})
	t.Run("read conversation keeps the newest, oldest first", func(t *testing.T) {
		t.Parallel()
		got := snap.ReadConversation("c1", "", 2)
		require.Len(t, got, 2)
		assert.Equal(t, "2", got[0].MessageID)
		assert.Equal(t, "1", got[1].MessageID)
	})
	t.Run("conversations need a title or a last message", func(t *testing.T) {
		t.Parallel()
		assert.Len(t, snap.ListConversations("", 0), 2)
		assert.Len(t, snap.ListConversations("b", 0), 1)
	})
	t.Run("mentions", func(t *testing.T) {
		t.Parallel()
		all := snap.MentionsQuery(0, false, "", 0, opts())
		require.Len(t, all, 2)
		assert.Equal(t, "Plan", all[0].Conversation)
		assert.Equal(t, "Budge", all[0].Content, "content is cut to MaxContent characters")
		assert.Equal(t, "c2", all[1].Conversation, "without a title, the id is shown")
		assert.Len(t, snap.MentionsQuery(0, true, "", 0, opts()), 1)
		assert.Len(t, snap.MentionsQuery(5, false, "", 0, opts()), 1)
	})
	t.Run("overview groups by conversation", func(t *testing.T) {
		t.Parallel()
		got := snap.Overview(7, "", opts())
		require.Len(t, got, 2)
		assert.Equal(t, "c3", got[0].ConversationID)
		assert.Equal(t, OverviewRow{
			Conversation: "Plan", ConversationID: "c1", Count: 2, Senders: []string{"Ann (Org)", "Bob"},
			LastSender: "Ann (Org)", LastMessage: "Budge", LastTime: ms(1), Account: "a",
		}, got[1], "the last message is cut to MaxContent characters")
		assert.Len(t, snap.Overview(0, "", opts()), 3, "0 days means no cutoff")
	})
}

func TestTruncate(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "héllo", Truncate("héllo", 5))
	assert.Equal(t, "hé…", Truncate("héllo", 2))
	assert.Equal(t, "héllo", Truncate("héllo", 0), "0 means no limit")
	assert.Equal(t, "héllo", Truncate("héllo", -1), "a negative limit does not panic")
	assert.Equal(t, "héllo", firstChars("héllo", -1))
}

func TestOptionsWithoutNow(t *testing.T) {
	t.Parallel()
	snap := &Snapshot{Messages: []teams.Message{msg("a", "c", "1", "Ann", ms(0), "hi")}}

	assert.Len(t, snap.Search("hi", "", 1, 0, Options{}), 1, "a nil Now means time.Now")
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

	got := snap.MentionsQuery(0, false, "", 0, Options{})

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

func TestOverviewPrefersTheNewerVersionOnEqualTimes(t *testing.T) {
	t.Parallel()
	ts := ms(1)
	older := msg("a", "c", "m", "Ann", ts, "before the edit")
	older.Seq = 3
	newer := msg("a", "c", "m", "Ann", ts, "after the edit")
	newer.Seq = 8
	undated := msg("a", "d", "x", "Bob", "", "no time")
	snap := &Snapshot{Messages: []teams.Message{older, newer, undated}}

	got := snap.Overview(0, "", Options{})

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

	got := snap.Search("budget", "", 0, 2, opts())

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
