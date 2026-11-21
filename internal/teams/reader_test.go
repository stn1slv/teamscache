package teams

import (
	"context"
	"iter"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stn1slv/teamscache/internal/idbtest"
	"github.com/stn1slv/teamscache/internal/leveldb"
	"github.com/stn1slv/teamscache/internal/v8"
)

// obj builds a decoded object from key, value pairs.
func obj(kv ...any) *v8.Object {
	o := v8.NewObject()
	for i := 0; i+1 < len(kv); i += 2 {
		o.Set(kv[i], kv[i+1])
	}
	return o
}

// fakeStores returns the same record values for every manager and store.
// Each value is a live record with its own key.
func fakeStores(values ...any) storeFunc {
	records := make([]storeRecord, 0, len(values))
	for i, v := range values {
		records = append(records, storeRecord{key: strconv.Itoa(i), seq: uint64(i + 1), live: true, value: v})
	}
	return fakeRecords(records...)
}

// fakeRecords returns the same record versions for every manager and store.
func fakeRecords(records ...storeRecord) storeFunc {
	return func(string, string) iter.Seq2[Account, iter.Seq[storeRecord]] {
		return func(yield func(Account, iter.Seq[storeRecord]) bool) {
			yield(Account{TenantID: "t", UserID: "u"}, func(y func(storeRecord) bool) {
				for _, r := range records {
					if !y(r) {
						return
					}
				}
			})
		}
	}
}

func TestMentionsAndConversationsUseTheNewestVersion(t *testing.T) {
	t.Parallel()
	mention := func(read bool) *v8.Object {
		return obj("sourceThreadId", "c", "sourceMessageId", "m", "isRead", read)
	}
	stores := fakeRecords(
		storeRecord{key: "a", seq: 9, live: true, value: mention(true)},
		storeRecord{key: "a", seq: 3, live: true, value: mention(false)}, // older, in a later file
		storeRecord{key: "b", seq: 4, live: true, value: mention(false)},
		storeRecord{key: "b", seq: 5, live: false},                        // deleted
		storeRecord{key: "c", seq: 6, live: true, value: obj("id", "c1")}, // only for conversations
	)

	got := mentions(stores, "")
	convs := conversations(stores, "")

	require.Len(t, got, 2, "record b is deleted")
	assert.True(t, got[0].IsRead, "the newest version of a mention wins, not the last in file order")
	assert.Len(t, convs, 2)
}

func TestMessagesComeFromTheNewestReplyChain(t *testing.T) {
	t.Parallel()
	chain := func(msgs ...string) *v8.Object {
		mm := v8.NewObject()
		for _, id := range msgs {
			mm.Set(id, obj("id", id, "content", "text of "+id))
		}
		return obj("messageMap", mm)
	}
	got := messages(fakeRecords(
		storeRecord{key: "r", seq: 1, live: true, value: chain("m1")},
		storeRecord{key: "r", seq: 2, live: true, value: chain("m1", "m2")}, // a reply was added
	), "")

	require.Len(t, got, 2, "m1 is not repeated from the older version")
	assert.Equal(t, []string{"m1", "m2"}, []string{got[0].MessageID, got[1].MessageID})
	assert.Equal(t, uint64(2), got[0].Seq)
}

func TestFailedNewestVersionHidesOlderVersions(t *testing.T) {
	t.Parallel()
	got := mentions(fakeRecords(
		storeRecord{key: "a", seq: 1, live: true, value: obj("sourceMessageId", "m", "isRead", false)},
		storeRecord{key: "a", seq: 2, live: true, failed: true},
	), "")

	assert.Empty(t, got, "an older unread state is not shown as current")
}

func TestConversationTitle(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		record *v8.Object
		want   string
	}{
		{"topic wins", obj("threadProperties", obj("topic", "Team chat"), "chatTitle", obj("shortTitle", "Ann")), "Team chat"},
		{"null topic", obj("threadProperties", obj("topic", nil), "chatTitle", obj("shortTitle", "Ann Lee")), "Ann Lee"},
		{"undefined topic", obj("threadProperties", obj("topic", v8.Undefined{}), "chatTitle", obj("shortTitle", "Ann Lee")), "Ann Lee"},
		{"long title", obj("chatTitle", obj("shortTitle", "", "longTitle", "Ann Lee, Bob Ray")), "Ann Lee, Bob Ray"},
		{"undefined titles", obj("chatTitle", obj("shortTitle", v8.Undefined{}, "longTitle", v8.Undefined{})), ""},
		{"unknown user", obj("chatTitle", obj("shortTitle", "Unknown", "hasUnknownUserInShortTitle", true)), ""},
		{"unknown user, long title", obj("chatTitle", obj("shortTitle", "Unknown", "longTitle", "Ann Lee, Bob Ray", "hasUnknownUserInShortTitle", true)), "Ann Lee, Bob Ray"},
		{"undefined chat title", obj("chatTitle", v8.Undefined{}), ""},
		{"undefined thread properties", obj("threadProperties", v8.Undefined{}), ""},
		{"empty", obj(), ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, conversationTitle(tt.record))
		})
	}
}

func TestConversationsYieldTheChatTitle(t *testing.T) {
	t.Parallel()
	stores := fakeStores(
		obj("id", "19:a_b@unq.gbl.spaces", "threadProperties", obj("topic", nil), "chatTitle", obj("shortTitle", "Ann Lee")),
		nil,
	)

	rows := conversations(stores, "")

	require.Len(t, rows, 1)
	assert.Equal(t, "19:a_b@unq.gbl.spaces", rows[0].ID)
	assert.Equal(t, "Ann Lee", rows[0].Title)
}

func TestRecordsThatAreNotObjectsAreSkipped(t *testing.T) {
	t.Parallel()
	for _, root := range []any{[]any{"item"}, v8.Undefined{}} {
		stores := fakeStores(root)
		assert.Empty(t, conversations(stores, ""))
		assert.Empty(t, messages(stores, ""))
		assert.Empty(t, mentions(stores, ""))
	}
}

func TestMessageEntriesThatAreNotObjectsAreSkipped(t *testing.T) {
	t.Parallel()
	for _, entry := range []any{v8.Undefined{}, nil, "text", []any{"item"}} {
		record := obj("messageMap", obj("bad", entry, "good", obj("content", "hi", "imDisplayName", "Ann (Org)")))

		got := messages(fakeStores(record), "")

		require.Len(t, got, 1)
		assert.Equal(t, "hi", got[0].Content)
	}
}

func TestMessageFields(t *testing.T) {
	t.Parallel()
	record := obj(
		"conversationId", "19:c@thread.v2",
		"replyChainId", 1768557031836.0,
		"messageMap", obj(
			int64(42), obj("content", "<p>no id</p>", "imDisplayName", "", "from", "8:orgid:x", "composetime", "2026-01-16T09:50:31.836Z"),
			"m2", obj("id", "m2", "content", "dated", "imDisplayName", "Ann", "originalArrivalTime", 1768557031836.0, "messageType", "RichText/Html"),
			"m3", obj("content", "   "),
		),
	)

	got := messages(fakeStores(record), "")

	require.Len(t, got, 2)
	assert.Equal(t, Message{
		Account: "t:u", ConversationID: "19:c@thread.v2", ReplyChainID: "1768557031836.0", MessageID: "42",
		Sender: "8:orgid:x", Timestamp: "2026-01-16T09:50:31.836Z", Content: "no id", ContentType: "", Seq: 1,
	}, got[0])
	assert.Equal(t, "1768557031836.0", got[1].Timestamp)
	assert.Equal(t, "RichText/Html", got[1].ContentType)
}

func TestPlainMessagesAndConversationPreviewsKeepMarkup(t *testing.T) {
	t.Parallel()
	body := "Keep <placeholder> &amp; x < 5 > 3"
	message := obj("content", body, "messageType", "Text")
	record := obj("messageMap", obj("m", message), "lastMessage", message)
	msgs := messages(fakeStores(record), "")
	convs := conversations(fakeStores(record), "")
	require.Len(t, msgs, 1)
	require.Len(t, convs, 1)
	assert.Equal(t, body, msgs[0].Content)
	assert.Equal(t, body, convs[0].LastMessage)
}

func TestMentionReadState(t *testing.T) {
	t.Parallel()
	got := mentions(fakeStores(
		obj("sourceThreadId", "c", "sourceMessageId", "m", "timestamp", 1.5, "isRead", true),
		obj("sourceThreadId", "c", "sourceMessageId", "n", "isRead", "true"),
	), "")

	require.Len(t, got, 2)
	assert.True(t, got[0].IsRead)
	assert.Equal(t, "1.5", got[0].Timestamp)
	assert.False(t, got[1].IsRead, "only the boolean true counts")
	assert.Empty(t, got[1].Timestamp)
}

// TestOpenReadsASyntheticCache runs the whole reader on a synthetic cache
// folder with Teams database names.
func TestOpenReadsASyntheticCache(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	dir := filepath.Join(root, "cache.leveldb")
	db := func(manager, tenant string) string {
		return "Teams:" + manager + ":react-web-client:" + tenant + ":user-" + tenant + ":en-us"
	}
	p := func(k string, v any) idbtest.Prop { return idbtest.Prop{Key: k, Value: v} }
	message := idbtest.Obj{p("id", "m1"), p("content", "<p>Hello</p>"), p("imDisplayName", "Ann Lee (Org)"), p("originalArrivalTime", 1768557031836.0)}
	require.NoError(t, idbtest.Write(dir, filepath.Join(root, "cache.blob"), []idbtest.Database{
		{Number: 1, Name: db("replychain-manager", "t1"), Stores: []idbtest.Store{{Name: "replychains", Records: []idbtest.Record{
			{Key: "r1", Value: idbtest.Obj{p("conversationId", "c1"), p("replyChainId", "r1"), p("messageMap", idbtest.Obj{p("m1", message)})}},
			{Key: "r2", Value: idbtest.Obj{p("conversationId", "c1"), p("messageMap", idbtest.Obj{p("m2", idbtest.Obj{p("content", "big")})})}, Wrapping: idbtest.Blob},
			{Key: "bad", Value: "x", Wrapping: idbtest.Corrupt},
		}}}},
		{Number: 2, Name: db("conversation-manager", "t1"), Stores: []idbtest.Store{{Name: "conversations", Records: []idbtest.Record{
			{Key: "c1", Value: idbtest.Obj{p("id", "c1"), p("threadProperties", idbtest.Obj{p("topic", "Project")})}},
		}}}},
		{Number: 3, Name: db("messaging-slice-manager", "t1"), Stores: []idbtest.Store{{Name: "mentions-metadata-items", Records: []idbtest.Record{
			{Key: "x", Value: idbtest.Obj{p("sourceThreadId", "c1"), p("sourceMessageId", "m1"), p("isRead", false)}},
		}}}},
		{Number: 4, Name: db("replychain-manager", "t2"), Stores: []idbtest.Store{{Name: "other-store"}}},
	}))

	r, err := Open(context.Background(), dir, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	tmp := r.tmp
	t.Cleanup(r.Close)
	require.NoError(t, os.Remove(filepath.Join(root, "cache.blob", "1", "00", "1")))

	assert.Equal(t, []Account{{TenantID: "t1", UserID: "user-t1"}}, r.Accounts())
	msgs := r.Messages("")
	require.Len(t, msgs, 2)
	assert.Equal(t, "Ann Lee (Org)", msgs[0].Sender)
	assert.Equal(t, "Hello", msgs[0].Content)
	assert.Equal(t, "big", msgs[1].Content, "blob values remain readable after the original file is removed")
	assert.Empty(t, r.Messages("other:account"))
	assert.Equal(t, "Project", r.Conversations("")[0].Title)
	assert.Equal(t, []Mention{{Account: "t1:user-t1", ConversationID: "c1", MessageID: "m1", ReplyChainID: "", Timestamp: "", IsRead: false}}, r.Mentions(""))
	assert.Equal(t, 1, r.Skipped())
	assert.Empty(t, r.Damage())

	r.Close()
	_, err = os.Stat(tmp)
	assert.True(t, os.IsNotExist(err), "Close removes the temporary copy")
}

func TestMissingBlobProducesPartialResults(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	dir, blobs := filepath.Join(root, "cache.leveldb"), filepath.Join(root, "cache.blob")
	p := func(k string, v any) idbtest.Prop { return idbtest.Prop{Key: k, Value: v} }
	require.NoError(t, idbtest.Write(dir, blobs, []idbtest.Database{{
		Number: 1, Name: "Teams:replychain-manager:react-web-client:t:u:en-us",
		Stores: []idbtest.Store{{Name: "replychains", Records: []idbtest.Record{
			{Key: "r", Wrapping: idbtest.Blob, Value: idbtest.Obj{
				p("messageMap", idbtest.Obj{p("m", idbtest.Obj{p("content", "message")})}),
			}},
		}}},
	}}))
	require.NoError(t, os.Remove(filepath.Join(blobs, "1", "00", "1")))

	r, err := Open(context.Background(), dir, slog.New(slog.DiscardHandler))

	require.NoError(t, err)
	t.Cleanup(r.Close)
	assert.Empty(t, r.Messages(""))
	assert.Equal(t, 1, r.Skipped())
	require.Len(t, r.Damage(), 1)
	assert.Equal(t, "referenced blob is missing", r.Damage()[0].Reason)
}

func TestCopyFileStopsWhenCanceled(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	src, dst := filepath.Join(root, "source"), filepath.Join(root, "copy")
	require.NoError(t, os.WriteFile(src, []byte("data"), 0o600))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, copyFile(ctx, src, dst, 0), context.Canceled)
	_, err := os.Stat(dst)
	assert.True(t, os.IsNotExist(err), "a canceled copy does not create an output file")
}

func TestOpenSkipsOversizedAndUnreadableBlobs(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"oversized", "unreadable"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			dir, blobs := filepath.Join(root, "cache.leveldb"), filepath.Join(root, "cache.blob")
			p := func(k string, v any) idbtest.Prop { return idbtest.Prop{Key: k, Value: v} }
			value := idbtest.Obj{p("messageMap", idbtest.Obj{p("m", idbtest.Obj{p("content", "message")})})}
			require.NoError(t, idbtest.Write(dir, blobs, []idbtest.Database{{
				Number: 1, Name: "Teams:replychain-manager:react-web-client:t:u:en-us",
				Stores: []idbtest.Store{{Name: "replychains", Records: []idbtest.Record{
					{Key: "good", Value: value},
					{Key: "blob", Value: value, Wrapping: idbtest.Blob},
				}}},
			}}))
			source := filepath.Join(blobs, "1", "00", "1")
			if kind == "oversized" {
				require.NoError(t, os.Truncate(source, leveldb.MaxDecodedSize+1))
			} else {
				require.NoError(t, os.Remove(source))
				require.NoError(t, os.Mkdir(source, 0o700))
			}

			r, err := Open(context.Background(), dir, slog.New(slog.DiscardHandler))

			require.NoError(t, err)
			t.Cleanup(r.Close)
			assert.Len(t, r.Messages(""), 1, "an unusable source blob does not hide other messages")
			assert.Equal(t, 1, r.Skipped())
			require.Len(t, r.Damage(), 1)
			if kind == "oversized" {
				assert.Contains(t, r.Damage()[0].Reason, "size limit")
			}
			_, err = os.Stat(filepath.Join(r.tmp, "blobs", "1", "00", "1"))
			assert.True(t, os.IsNotExist(err), "rejected or incomplete blob copies are removed")
		})
	}
}

func TestCopyFileLimitsAndDestinationErrors(t *testing.T) {
	t.Parallel()
	for _, size := range []int{7, 8, 9} {
		root := t.TempDir()
		src, dst := filepath.Join(root, "source"), filepath.Join(root, "copy")
		require.NoError(t, os.WriteFile(src, []byte(strings.Repeat("x", size)), 0o600))
		err := copyFile(context.Background(), src, dst, 8)
		if size > 8 {
			require.ErrorIs(t, err, ErrBlobTooLarge)
			_, statErr := os.Stat(dst)
			assert.True(t, os.IsNotExist(statErr))
		} else {
			require.NoError(t, err)
			info, statErr := os.Stat(dst)
			require.NoError(t, statErr)
			assert.EqualValues(t, size, info.Size())
		}
	}
	root := t.TempDir()
	src, dst := filepath.Join(root, "source"), filepath.Join(root, "destination")
	require.NoError(t, os.WriteFile(src, []byte("data"), 0o600))
	require.NoError(t, os.Mkdir(dst, 0o700))
	err := copyFile(context.Background(), src, dst, 8)
	require.Error(t, err)
	var sourceErr *sourceReadError
	assert.NotErrorAs(t, err, &sourceErr, "destination failures remain fatal")
}

func TestOpenWithoutCache(t *testing.T) {
	t.Parallel()
	_, err := Open(context.Background(), filepath.Join(t.TempDir(), "missing"), slog.New(slog.DiscardHandler))
	require.ErrorIs(t, err, ErrCacheNotFound)
}

func TestCopyDataFilesSkipsOtherFiles(t *testing.T) {
	t.Parallel()
	src, dst := t.TempDir(), filepath.Join(t.TempDir(), "copy")
	for _, name := range []string{"000003.log", "000005.ldb", "LOCK", "CURRENT", "MANIFEST-000001"} {
		require.NoError(t, os.WriteFile(filepath.Join(src, name), []byte("x"), 0o600))
	}

	require.NoError(t, copyDataFiles(context.Background(), src, dst, slog.New(slog.DiscardHandler)))

	entries, err := os.ReadDir(dst)
	require.NoError(t, err)
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	assert.Equal(t, []string{"000003.log", "000005.ldb"}, names)
}

func TestBlobDirFor(t *testing.T) {
	t.Parallel()
	want := filepath.Join("a", "x.blob")
	assert.Equal(t, want, blobDirFor(filepath.Join("a", "x.leveldb")))
	assert.Equal(t, want, blobDirFor(filepath.Join("a", "x.leveldb")+string(filepath.Separator)), "a trailing separator is ignored")
}

func TestDatabasesWithUnexpectedNamesAreSkipped(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "cache.leveldb")
	p := func(k string, v any) idbtest.Prop { return idbtest.Prop{Key: k, Value: v} }
	require.NoError(t, idbtest.Write(dir, "", []idbtest.Database{{
		Number: 1, Name: "Teams:replychain-manager:short",
		Stores: []idbtest.Store{{Name: "replychains", Records: []idbtest.Record{
			{Key: "r", Value: idbtest.Obj{p("messageMap", idbtest.Obj{p("m", idbtest.Obj{p("content", "hi")})})}},
		}}},
	}}))

	r, err := Open(context.Background(), dir, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	t.Cleanup(r.Close)

	assert.Empty(t, r.Accounts())
	assert.Empty(t, r.Messages(""))
}

func TestOpenStopsWhenCanceled(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "sample.leveldb")
	require.NoError(t, idbtest.WriteTeamsSample(dir, time.Now()))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := Open(ctx, dir, slog.New(slog.DiscardHandler))

	require.ErrorIs(t, err, context.Canceled)
}

func TestCopyOnceIsStableWhenNothingChanges(t *testing.T) {
	t.Parallel()
	src, dst := t.TempDir(), filepath.Join(t.TempDir(), "copy")
	require.NoError(t, os.WriteFile(filepath.Join(src, "000003.log"), []byte("x"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(src, "000004.ldb"), []byte("y"), 0o600))

	stable, err := copyOnce(context.Background(), src, dst)

	require.NoError(t, err)
	assert.True(t, stable)
	entries, err := os.ReadDir(dst)
	require.NoError(t, err)
	assert.Len(t, entries, 2)
}
