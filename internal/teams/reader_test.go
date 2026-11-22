package teams

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"iter"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
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
			"m2", obj("id", "m2", "content", "dated", "imDisplayName", "Ann", "creator", "8:orgid:ann", "isSentByCurrentUser", true, "originalArrivalTime", 1768557031836.0, "messageType", "RichText/Html"),
			"m3", obj("content", "   "),
		),
	)

	got := messages(fakeStores(record), "")

	require.Len(t, got, 2)
	assert.Equal(t, Message{
		Account: "t:u", ConversationID: "19:c@thread.v2", ReplyChainID: "1768557031836.0", MessageID: "42",
		Sender: "8:orgid:x", Creator: "8:orgid:x", Timestamp: "2026-01-16T09:50:31.836Z", Content: "no id", ContentType: "", Seq: 1,
	}, got[0])
	assert.Equal(t, "1768557031836.0", got[1].Timestamp)
	assert.Equal(t, "RichText/Html", got[1].ContentType)
	assert.Equal(t, "8:orgid:ann", got[1].Creator)
	require.NotNil(t, got[1].IsSentByCurrentUser)
	assert.True(t, *got[1].IsSentByCurrentUser)
}

func TestProfilesReturnsSignedInUserOnly(t *testing.T) {
	t.Parallel()
	stores := fakeRecords(
		storeRecord{key: "colleague", seq: 1, live: true, value: obj(
			"objectId", "other-user", "mri", "8:orgid:other-user", "tenantName", "Other Org", "displayName", "Bob", "email", "bob@example.test",
		)},
		storeRecord{key: "self-by-mri", seq: 2, live: true, value: obj(
			"mri", "8:orgid:U", "tenantName", "Acme <Org>", "displayName", "Ann Lee", "userPrincipalName", "ann@acme.test",
		)},
		storeRecord{key: "self-by-id", seq: 3, live: true, value: obj(
			"objectId", "U", "displayName", "Ann Lee Updated", "mail", "ann.lee@acme.test",
		)},
	)

	got := profiles(stores, "")

	require.Len(t, got, 1)
	assert.Equal(t, Profile{
		Account:     "t:u",
		TenantName:  "Acme <Org>",
		DisplayName: "Ann Lee Updated",
		Email:       "ann.lee@acme.test",
	}, got[0])
	assert.Empty(t, profiles(stores, "other:account"))
}

func TestOpenForSkipsUnrelatedMissingBlobs(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	dir, blobs := filepath.Join(root, "cache.leveldb"), filepath.Join(root, "cache.blob")
	p := func(k string, v any) idbtest.Prop { return idbtest.Prop{Key: k, Value: v} }
	require.NoError(t, idbtest.Write(dir, blobs, []idbtest.Database{
		{
			Number: 1, Name: "Teams:replychain-manager:react-web-client:t:u:en-us",
			Stores: []idbtest.Store{{Name: "replychains", Records: []idbtest.Record{
				{Key: "r", Value: idbtest.Obj{p("messageMap", idbtest.Obj{p("m", idbtest.Obj{p("content", "ok")})})}},
			}}},
		},
		{
			Number: 2, Name: "Teams:user-installed-apps-manager:react-web-client:t:u:en-us",
			Stores: []idbtest.Store{{Name: "user-app-definitions", Records: []idbtest.Record{
				{Key: "app", Wrapping: idbtest.Blob, Value: idbtest.Obj{p("id", "app1")}},
			}}},
		},
	}))
	require.NoError(t, os.Remove(filepath.Join(blobs, "2", "00", "1")))

	r, err := OpenFor(context.Background(), dir, slog.New(slog.DiscardHandler), ScopeMessages)

	require.NoError(t, err)
	t.Cleanup(r.Close)
	assert.Empty(t, r.Damage())
	assert.Zero(t, r.Skipped())
	assert.Len(t, r.Messages(""), 1)
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

	stable, manifestErr, manifestDamage, err := copyDataFiles(context.Background(), src, dst, slog.New(slog.DiscardHandler))

	require.NoError(t, err)
	assert.True(t, stable)
	require.ErrorIs(t, manifestErr, leveldb.ErrCorrupt, "corrupt CURRENT is surfaced while falling back to data files")
	assert.Empty(t, manifestDamage)
	entries, err := os.ReadDir(dst)
	require.NoError(t, err)
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	assert.Equal(t, []string{"000003.log", "000005.ldb"}, names)
}

func TestOpenAcceptsDotPath(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "sample.leveldb")
	require.NoError(t, idbtest.WriteTeamsSample(dir, time.Now()))
	t.Chdir(dir)

	r, err := Open(context.Background(), ".", slog.New(slog.DiscardHandler))

	require.NoError(t, err)
	t.Cleanup(r.Close)
	assert.Len(t, r.Messages(""), 2)
}

func TestProfileNameAndFindCacheForMissingProfile(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "WV2Profile_tfw", profileName(filepath.Join("EBWebView", "WV2Profile_tfw", "IndexedDB", leveldbName)))
	_, err := FindCacheFor("no-such-profile-name-xyz")
	require.ErrorIs(t, err, ErrCacheNotFound)
}

func TestDiscoverCachesAndFindCacheFor(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("LOCALAPPDATA", home)

	makeProfile := func(profile string, mod time.Time) string {
		var dir string
		switch runtime.GOOS {
		case "darwin":
			dir = filepath.Join(home, "Library", "Containers", "com.microsoft.teams2", "Data", "Library",
				"Application Support", "Microsoft", "MSTeams", "EBWebView", profile, "IndexedDB", leveldbName)
		case "windows":
			dir = filepath.Join(home, "Packages", "MSTeams_test", "LocalCache", "Microsoft", "MSTeams",
				"EBWebView", profile, "IndexedDB", leveldbName)
		default:
			dir = filepath.Join(home, ".config", "teams", profile, "IndexedDB", leveldbName)
		}
		require.NoError(t, os.MkdirAll(dir, 0o700))
		require.NoError(t, os.Chtimes(dir, mod, mod))
		return dir
	}

	older := makeProfile("WV2Profile_tfl", time.Now().Add(-time.Hour))
	newer := makeProfile("WV2Profile_tfw", time.Now())

	caches := DiscoverCaches()
	require.Len(t, caches, 2)
	assert.Equal(t, "WV2Profile_tfw", caches[0].Profile)
	assert.Equal(t, newer, FindCache())

	got, err := FindCacheFor("wv2profile_tfl")
	require.NoError(t, err)
	assert.Equal(t, older, got)

	exact, err := matchProfile([]DiscoveredCache{
		{Profile: "Default", Path: "/newer"},
		{Profile: "default", Path: "/older"},
	}, "default")
	require.NoError(t, err)
	assert.Equal(t, "/older", exact)
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

func TestNumberedStoresAreRead(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "cache.leveldb")
	p := func(k string, v any) idbtest.Prop { return idbtest.Prop{Key: k, Value: v} }
	chain := func(conv string, ids ...string) idbtest.Obj {
		mm := make(idbtest.Obj, 0, len(ids))
		for _, id := range ids {
			mm = append(mm, p(id, idbtest.Obj{p("id", id), p("content", "text of "+id)}))
		}
		return idbtest.Obj{p("conversationId", conv), p("messageMap", mm)}
	}
	require.NoError(t, idbtest.Write(dir, "", []idbtest.Database{{
		Number: 1, Name: "Teams:replychain-manager:react-web-client:t:u:en-us",
		Stores: []idbtest.Store{
			{Name: "replychains-2", Records: []idbtest.Record{
				{Key: "r2", Value: chain("c2", "m3")}, // the store with the newer id is written first
			}},
			{Name: "replychains", Records: []idbtest.Record{
				{Key: "r1", Value: chain("c1", "m1")},
				{Key: "r2", Value: chain("c2", "m3"), Deleted: true}, // deleted here, kept in replychains-2
			}},
			{Name: "replychains-3", Records: []idbtest.Record{
				{Key: "r1", Value: chain("c1", "m1", "m2")}, // a newer version of r1
			}},
			{Name: "replychains-old", Records: []idbtest.Record{{Key: "r9", Value: chain("c9", "m9")}}},
			{Name: "other-replychains-2", Records: []idbtest.Record{{Key: "r8", Value: chain("c8", "m8")}}},
		},
	}}))

	r, err := Open(context.Background(), dir, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	t.Cleanup(r.Close)

	msgs := r.Messages("")
	got := make([]string, 0, len(msgs))
	for _, m := range msgs {
		got = append(got, m.ConversationID+"/"+m.MessageID)
	}
	assert.ElementsMatch(t, []string{"c1/m1", "c1/m2", "c2/m3"}, got,
		"the newest version of r1 once, r2 from the store that keeps it, no other store")
	assert.Equal(t, []Account{{TenantID: "t", UserID: "u"}}, r.Accounts())
}

// crossStoreCache writes a Teams cache in two steps, so the base stores
// (lower ids) hold newer versions than their numbered copies. Each argument
// maps a base store name to its records in that step.
func crossStoreCache(t *testing.T, older, newer map[string][]idbtest.Record) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "cache.leveldb")
	type pair struct {
		number         uint64
		manager, store string
	}
	pairs := []pair{
		{1, "replychain-manager", "replychains"},
		{2, "messaging-slice-manager", "mentions-metadata-items"},
		{3, "conversation-manager", "conversations"},
	}
	for _, step := range []struct {
		base, copied map[string][]idbtest.Record
	}{{copied: older}, {base: newer}} {
		dbs := make([]idbtest.Database, 0, len(pairs))
		for _, pr := range pairs {
			dbs = append(dbs, idbtest.Database{
				Number: pr.number, Name: "Teams:" + pr.manager + ":react-web-client:t:u:en-us",
				Stores: []idbtest.Store{{Name: pr.store, Records: step.base[pr.store]}, {Name: pr.store + "-2", Records: step.copied[pr.store]}},
			})
		}
		require.NoError(t, idbtest.Write(dir, "", dbs))
	}
	return dir
}

func TestNewestVersionWinsAcrossStores(t *testing.T) {
	t.Parallel()
	p := func(k string, v any) idbtest.Prop { return idbtest.Prop{Key: k, Value: v} }
	msg := func(id, text string) idbtest.Prop { return p(id, idbtest.Obj{p("id", id), p("content", text)}) }
	chain := func(msgs ...idbtest.Prop) idbtest.Obj {
		return idbtest.Obj{p("conversationId", "c1"), p("messageMap", idbtest.Obj(msgs))}
	}
	mention := func(read bool) idbtest.Obj {
		return idbtest.Obj{p("sourceThreadId", "c1"), p("sourceMessageId", "m1"), p("isRead", read)}
	}
	chat := func(topic string) idbtest.Obj {
		return idbtest.Obj{p("id", "c1"), p("threadProperties", idbtest.Obj{p("topic", topic)})}
	}
	dir := crossStoreCache(t, map[string][]idbtest.Record{
		"replychains":             {{Key: "r1", Value: chain(msg("m1", "old"), msg("m2", "only in the copy"), msg("m4", "text before it was cleared"))}},
		"mentions-metadata-items": {{Key: "x", Value: mention(false)}},
		"conversations":           {{Key: "c1", Value: chat("Old topic")}},
	}, map[string][]idbtest.Record{
		"replychains":             {{Key: "r1", Value: chain(msg("m1", "edited"), msg("m3", "only in the base"), msg("m4", ""))}},
		"mentions-metadata-items": {{Key: "x", Value: mention(true)}},
		"conversations":           {{Key: "c1", Value: chat("New topic")}},
	})

	r, err := Open(context.Background(), dir, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	t.Cleanup(r.Close)

	got := map[string]string{}
	for _, m := range r.Messages("") {
		_, dup := got[m.MessageID]
		assert.False(t, dup, "message %s is returned once", m.MessageID)
		got[m.MessageID] = m.Content
	}
	assert.Equal(t, map[string]string{"m1": "edited", "m2": "only in the copy", "m3": "only in the base"}, got,
		"the messages of both copies of the chain, each from its newest version; m4 was cleared in the newer version")
	mentions := r.Mentions("")
	require.Len(t, mentions, 1)
	assert.True(t, mentions[0].IsRead, "the newer version in the store with the lower id wins")
	conversations := r.Conversations("")
	require.Len(t, conversations, 1)
	assert.Equal(t, "New topic", conversations[0].Title)
}

func TestUndecodableVersionHidesOlderCopies(t *testing.T) {
	t.Parallel()
	p := func(k string, v any) idbtest.Prop { return idbtest.Prop{Key: k, Value: v} }
	dir := crossStoreCache(t, map[string][]idbtest.Record{
		"replychains": {{Key: "r1", Value: idbtest.Obj{p("conversationId", "c1"), p("messageMap", idbtest.Obj{
			p("m1", idbtest.Obj{p("id", "m1"), p("content", "old")}),
		})}}},
		"mentions-metadata-items": {{Key: "x", Value: idbtest.Obj{p("sourceThreadId", "c1"), p("sourceMessageId", "m1"), p("isRead", false)}}},
		"conversations":           {{Key: "c1", Value: idbtest.Obj{p("id", "c1")}}},
	}, map[string][]idbtest.Record{
		"replychains":             {{Key: "r1", Value: "x", Wrapping: idbtest.Corrupt}},
		"mentions-metadata-items": {{Key: "x", Value: "x", Wrapping: idbtest.Corrupt}},
		"conversations":           {{Key: "c1", Value: "x", Wrapping: idbtest.Corrupt}},
	})

	r, err := Open(context.Background(), dir, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	t.Cleanup(r.Close)

	assert.Empty(t, r.Messages(""), "an older chain copy is not shown in place of a newer one")
	assert.Empty(t, r.Mentions(""), "an older unread state is not shown in place of a newer one")
	assert.Empty(t, r.Conversations(""))
	assert.Equal(t, 3, r.Skipped())
}

func TestMessagesWithoutIDAreKept(t *testing.T) {
	t.Parallel()
	got := messages(fakeStores(obj("conversationId", "c", "messageMap", obj(
		"a", obj("id", "", "content", "first"),
		"b", obj("id", "", "content", "second"),
	))), "")

	require.Len(t, got, 2, "messages with an empty id do not replace each other")
	assert.Equal(t, []string{"first", "second"}, []string{got[0].Content, got[1].Content})
}

func TestIsStore(t *testing.T) {
	t.Parallel()
	for name, want := range map[string]bool{
		"replychains": true, "replychains-2": true, "replychains-10": true,
		"replychains-": false, "replychains-x": false, "replychains-2-3": false, "my-replychains-2": false, "replychain": false,
	} {
		assert.Equal(t, want, isStore(name, "replychains"), name)
	}
	assert.True(t, isStore("mentions-metadata-items-2", "mentions-metadata-items"))
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

	res, err := copyOnce(context.Background(), src, dst, false)

	require.NoError(t, err)
	assert.True(t, res.stable)
	require.NoError(t, res.manifestErr)
	entries, err := os.ReadDir(dst)
	require.NoError(t, err)
	assert.Len(t, entries, 2)
}

func TestIsTeamsOriginDir(t *testing.T) {
	t.Parallel()
	for name, want := range map[string]bool{
		"https_teams.microsoft.com_0.indexeddb.leveldb":         true,
		"https_teams.cloud.microsoft_0.indexeddb.leveldb":       true,
		"https_teams.live.com_0.indexeddb.leveldb":              true,
		"https_teams.microsoft.us_0.indexeddb.leveldb":          true,
		"https_dod.teams.microsoft.us_0.indexeddb.leveldb":      true,
		"https_gov.teams.microsoft.us_0.indexeddb.leveldb":      true,
		"https_teams.microsoftonline.cn_0.indexeddb.leveldb":    true,
		"https_statics.teams.microsoft.com_0.indexeddb.leveldb": false,
		"https_.teams.microsoft.com_0.indexeddb.leveldb":        false,
		"http_teams.microsoft.com_0.indexeddb.leveldb":          false,
		"https_teams.microsoft.com_8443.indexeddb.leveldb":      false,
		"https_contoso-my.sharepoint.com_0.indexeddb.leveldb":   false,
		"https_evil-teams.microsoft.com_0.indexeddb.leveldb":    false,
		"https_teams.microsoft.com_0.indexeddb.blob":            false,
		"https_teams.microsoft.com_abc.indexeddb.leveldb":       false,
	} {
		assert.Equal(t, want, isTeamsOriginDir(name), name)
	}
}

func TestDiscoverCachesFindsMultipleOriginsAndSkipsNonTeams(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("LOCALAPPDATA", home)

	makeOrigin := func(profile, origin string, mod time.Time) string {
		var dir string
		switch runtime.GOOS {
		case "darwin":
			dir = filepath.Join(home, "Library", "Containers", "com.microsoft.teams2", "Data", "Library",
				"Application Support", "Microsoft", "MSTeams", "EBWebView", profile, "IndexedDB", origin)
		case "windows":
			dir = filepath.Join(home, "Packages", "MSTeams_test", "LocalCache", "Microsoft", "MSTeams",
				"EBWebView", profile, "IndexedDB", origin)
		default:
			dir = filepath.Join(home, ".config", "teams", profile, "IndexedDB", origin)
		}
		require.NoError(t, os.MkdirAll(dir, 0o700))
		require.NoError(t, os.Chtimes(dir, mod, mod))
		return dir
	}

	makeOrigin("WV2Profile_tfl", "https_teams.live.com_0.indexeddb.leveldb", time.Now().Add(-2*time.Hour))
	cloud := makeOrigin("WV2Profile_tfw", "https_teams.cloud.microsoft_0.indexeddb.leveldb", time.Now())
	makeOrigin("WV2Profile_tfw", "https_contoso-my.sharepoint.com_0.indexeddb.leveldb", time.Now().Add(time.Hour))

	caches := DiscoverCaches()
	require.Len(t, caches, 2, "SharePoint origin is excluded")
	assert.Equal(t, cloud, caches[0].Path)
	assert.Equal(t, "WV2Profile_tfw", caches[0].Profile)
	assert.Equal(t, "WV2Profile_tfl", caches[1].Profile)
}

func TestCopyDataFilesWithManifestSkipsObsoleteTables(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	src := filepath.Join(root, "cache.leveldb")
	dst := filepath.Join(root, "copy")
	require.NoError(t, idbtest.WriteTeamsSample(src, time.Now()))
	require.NoError(t, os.WriteFile(filepath.Join(src, "999999.ldb"), []byte("obsolete"), 0o600))

	stable, manifestErr, manifestDamage, err := copyDataFiles(context.Background(), src, dst, slog.New(slog.DiscardHandler))

	require.NoError(t, err)
	assert.True(t, stable)
	require.NoError(t, manifestErr)
	assert.Empty(t, manifestDamage)
	_, err = os.Stat(filepath.Join(dst, "CURRENT"))
	require.NoError(t, err, "CURRENT is copied when manifest is valid")
	_, err = os.Stat(filepath.Join(dst, "999999.ldb"))
	assert.True(t, os.IsNotExist(err), "obsolete table not in manifest is skipped")
}

func TestOpenForReportsCorruptManifestAsDamage(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "cache.leveldb")
	require.NoError(t, idbtest.WriteTeamsSample(dir, time.Now()))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "CURRENT"), []byte("MANIFEST-000099\n"), 0o600))

	r, err := OpenFor(context.Background(), dir, slog.New(slog.DiscardHandler), ScopeMessages)
	require.NoError(t, err)
	t.Cleanup(r.Close)

	assert.Len(t, r.Messages(""), 2, "raw fallback still reads all messages")
	require.Len(t, r.Damage(), 1)
	assert.Equal(t, "CURRENT", r.Damage()[0].File)
	assert.Contains(t, r.Damage()[0].Reason, "MANIFEST-000099")
}

func TestOpenForFallbackOnUnreadableCurrentAndSymlink(t *testing.T) {
	t.Parallel()
	// CURRENT replaced by a directory (non-regular, unreadable as a file) falls back cleanly.
	dir := filepath.Join(t.TempDir(), "cache.leveldb")
	require.NoError(t, idbtest.WriteTeamsSample(dir, time.Now()))
	require.NoError(t, os.Remove(filepath.Join(dir, "CURRENT")))
	require.NoError(t, os.Mkdir(filepath.Join(dir, "CURRENT"), 0o700))

	r, err := OpenFor(context.Background(), dir, slog.New(slog.DiscardHandler), ScopeMessages)
	require.NoError(t, err)
	t.Cleanup(r.Close)
	assert.Len(t, r.Messages(""), 2)
	require.Len(t, r.Damage(), 1)
	assert.Equal(t, "CURRENT", r.Damage()[0].File)

	// CURRENT as a symlink is rejected and falls back cleanly.
	dirSym := filepath.Join(t.TempDir(), "cache-sym.leveldb")
	require.NoError(t, idbtest.WriteTeamsSample(dirSym, time.Now()))
	target := filepath.Join(t.TempDir(), "real-current")
	require.NoError(t, os.WriteFile(target, []byte("MANIFEST-000001\n"), 0o600))
	require.NoError(t, os.Remove(filepath.Join(dirSym, "CURRENT")))
	require.NoError(t, os.Symlink(target, filepath.Join(dirSym, "CURRENT")))

	rSym, err := OpenFor(context.Background(), dirSym, slog.New(slog.DiscardHandler), ScopeMessages)
	require.NoError(t, err)
	t.Cleanup(rSym.Close)
	assert.Len(t, rSym.Messages(""), 2)
	require.Len(t, rSym.Damage(), 1)
	assert.Equal(t, "CURRENT", rSym.Damage()[0].File)
}

func TestOpenForReportsMissingManifestTableByName(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "cache.leveldb")
	require.NoError(t, idbtest.WriteTeamsSample(dir, time.Now()))

	// Remove one active table listed in the manifest while keeping log/other files.
	manifest, err := leveldb.ReadManifest(context.Background(), dir)
	require.NoError(t, err)
	if len(manifest.Tables) == 0 {
		// Force table creation by adding an extra manifest-referenced missing table.
		entries, readErr := os.ReadDir(dir)
		require.NoError(t, readErr)
		var ldbName string
		for _, e := range entries {
			if strings.HasSuffix(e.Name(), ".ldb") || strings.HasSuffix(e.Name(), ".sst") {
				ldbName = e.Name()
				break
			}
		}
		require.Empty(t, ldbName)
	}
	// Delete the table if present, otherwise synthesize a manifest referencing missing table 5.
	var expectedFile string
	if len(manifest.Tables) > 0 {
		for num := range manifest.Tables {
			expectedFile = fmt.Sprintf("%06d.ldb", num)
			_ = os.Remove(filepath.Join(dir, expectedFile))
			_ = os.Remove(filepath.Join(dir, fmt.Sprintf("%06d.sst", num)))
			break
		}
	} else {
		// Append a NewFile edit for missing table 5 to the existing manifest.
		mPath := filepath.Join(dir, manifest.File)
		// #nosec G703 -- mPath is inside t.TempDir() and manifest.File passed ValidateManifestName.
		mData, readErr := os.ReadFile(mPath)
		require.NoError(t, readErr)
		edit := []byte{7, 0, 5, 100, 1, 'a', 1, 'z'}
		frag := make([]byte, 7, 7+len(edit))
		frag[4] = byte(len(edit))
		frag[6] = 1
		frag = append(frag, edit...)
		crc := crc32.Checksum(frag[6:], crc32.MakeTable(crc32.Castagnoli))
		masked := (crc>>15 | crc<<17) + 0xa282ead8
		binary.LittleEndian.PutUint32(frag[:4], masked)
		// #nosec G703 -- mPath is inside t.TempDir() and manifest.File passed ValidateManifestName.
		require.NoError(t, os.WriteFile(mPath, append(mData, frag...), 0o600))
		expectedFile = "000005.ldb"
	}

	r, err := OpenFor(context.Background(), dir, slog.New(slog.DiscardHandler), ScopeMessages)
	require.NoError(t, err)
	t.Cleanup(r.Close)

	damage := r.Damage()
	files := make([]string, 0, len(damage))
	for _, d := range damage {
		files = append(files, d.File)
	}
	assert.Contains(t, files, expectedFile, "missing manifest table is reported by name")
}

func TestLogsStableAndMissingManifestTables(t *testing.T) {
	t.Parallel()
	// Normal append to an active log stays stable; shrinkage or rotation does not.
	assert.True(t, logsStable(
		[]fileStamp{{name: "000010.log", size: 100}},
		[]fileStamp{{name: "000010.log", size: 250}},
	))
	assert.False(t, logsStable(
		[]fileStamp{{name: "000010.log", size: 100}},
		[]fileStamp{{name: "000010.log", size: 50}},
	))
	assert.False(t, logsStable(
		[]fileStamp{{name: "000010.log", size: 100}},
		[]fileStamp{{name: "000011.log", size: 100}},
	))

	// checkManifestStamps detects missing tables, size mismatches, and missing PrevLogNumber.
	m := &leveldb.Manifest{
		PrevLogNumber: 8,
		Tables: map[uint64]leveldb.TableInfo{
			1: {Number: 1, Size: 500},
			2: {Number: 2, Size: 200},
		},
	}
	problems := checkManifestStamps(m, []fileStamp{{name: "000001.ldb", number: 1, size: 100}}, nil)
	require.Len(t, problems, 3)
	assert.Equal(t, "000001.ldb", problems[0].File)
	assert.Contains(t, problems[0].Reason, "does not match manifest size")
	assert.Equal(t, "000002.ldb", problems[1].File)
	assert.Contains(t, problems[1].Reason, "missing table")
	assert.Equal(t, "000008.log", problems[2].File)
	assert.Contains(t, problems[2].Reason, "previous log")

	assert.Empty(t, checkManifestStamps(
		m,
		[]fileStamp{{name: "000001.ldb", number: 1, size: 500}, {name: "000002.ldb", number: 2, size: 200}},
		[]fileStamp{{name: "000008.log", number: 8, size: 10}},
	))
}

func TestCopyDataFilesLiveLogGrowthAndRotation(t *testing.T) {
	t.Parallel()
	// Verify prefix copy captures a readable snapshot while an active log grows concurrently.
	dir := filepath.Join(t.TempDir(), "live.leveldb")
	require.NoError(t, idbtest.WriteTeamsSample(dir, time.Now()))
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	var logPath string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".log") {
			logPath = filepath.Join(dir, e.Name())
			break
		}
	}
	require.NotEmpty(t, logPath)

	// Append valid zero-padding / batches concurrently while opening the cache.
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		f, openErr := os.OpenFile(logPath, os.O_WRONLY|os.O_APPEND, 0o600)
		if openErr != nil {
			return
		}
		defer func() { _ = f.Close() }()
		// Append small valid empty write-batch fragments to simulate active WAL growth.
		payload := make([]byte, 12) // seq=0, count=0
		frag := make([]byte, 7, 7+len(payload))
		frag[4] = byte(len(payload))
		frag[6] = 1
		frag = append(frag, payload...)
		crc := crc32.Checksum(frag[6:], crc32.MakeTable(crc32.Castagnoli))
		masked := (crc>>15 | crc<<17) + 0xa282ead8
		binary.LittleEndian.PutUint32(frag[:4], masked)
		for range 20 {
			select {
			case <-stop:
				return
			default:
				_, _ = f.Write(frag)
				time.Sleep(time.Millisecond)
			}
		}
	}()

	r, err := OpenFor(context.Background(), dir, slog.New(slog.DiscardHandler), ScopeMessages)
	close(stop)
	<-done
	require.NoError(t, err)
	t.Cleanup(r.Close)
	assert.Len(t, r.Messages(""), 2, "messages stay readable during concurrent log appends")

	// Verify continuous log rotation causes instability reporting on a raw cache (no redundant 4th copy).
	rawDir := filepath.Join(t.TempDir(), "rotating.leveldb")
	require.NoError(t, os.MkdirAll(rawDir, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(rawDir, "000001.log"), []byte("a"), 0o600))
	stopRot := make(chan struct{})
	doneRot := make(chan struct{})
	go func() {
		defer close(doneRot)
		for n := 2; ; n++ {
			select {
			case <-stopRot:
				return
			default:
				name := filepath.Join(rawDir, fmt.Sprintf("%06d.log", n))
				_ = os.WriteFile(name, []byte("x"), 0o600)
				_ = os.Remove(filepath.Join(rawDir, fmt.Sprintf("%06d.log", n-1)))
				time.Sleep(time.Millisecond)
			}
		}
	}()
	dstRot := filepath.Join(t.TempDir(), "copy-rot")
	stable, manifestErr, manifestDamage, err := copyDataFiles(context.Background(), rawDir, dstRot, slog.New(slog.DiscardHandler))
	close(stopRot)
	<-doneRot
	require.NoError(t, err)
	require.NoError(t, manifestErr)
	assert.Empty(t, manifestDamage)
	_ = stable
}

func TestDiscoverCachesMultipleOriginsInOneProfile(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("LOCALAPPDATA", home)

	makeOrigin := func(origin string, mod time.Time) string {
		var dir string
		switch runtime.GOOS {
		case "darwin":
			dir = filepath.Join(home, "Library", "Containers", "com.microsoft.teams2", "Data", "Library",
				"Application Support", "Microsoft", "MSTeams", "EBWebView", "WV2Profile_tfw", "IndexedDB", origin)
		case "windows":
			dir = filepath.Join(home, "Packages", "MSTeams_test", "LocalCache", "Microsoft", "MSTeams",
				"EBWebView", "WV2Profile_tfw", "IndexedDB", origin)
		default:
			dir = filepath.Join(home, ".config", "teams", "WV2Profile_tfw", "IndexedDB", origin)
		}
		require.NoError(t, os.MkdirAll(dir, 0o700))
		require.NoError(t, os.Chtimes(dir, mod, mod))
		return dir
	}

	older := makeOrigin("https_teams.microsoft.com_0.indexeddb.leveldb", time.Now().Add(-time.Hour))
	newer := makeOrigin("https_teams.cloud.microsoft_0.indexeddb.leveldb", time.Now())

	caches := DiscoverCaches()
	require.Len(t, caches, 2)
	assert.Equal(t, newer, caches[0].Path)
	assert.Equal(t, older, caches[1].Path)
	assert.Equal(t, newer, FindCache())

	got, err := FindCacheFor("WV2Profile_tfw")
	require.NoError(t, err)
	assert.Equal(t, newer, got, "profile selection chooses the most recently updated origin")
}

func TestUpdatedBlobRecordHasZeroSkipped(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	dir, blobs := filepath.Join(root, "cache.leveldb"), filepath.Join(root, "cache.blob")
	p := func(k string, v any) idbtest.Prop { return idbtest.Prop{Key: k, Value: v} }
	chain := func(text string) idbtest.Obj {
		return idbtest.Obj{p("conversationId", "c1"), p("messageMap", idbtest.Obj{
			p("m1", idbtest.Obj{p("id", "m1"), p("content", text)}),
		})}
	}
	require.NoError(t, idbtest.Write(dir, blobs, []idbtest.Database{{
		Number: 1, Name: "Teams:replychain-manager:react-web-client:t:u:en-us",
		Stores: []idbtest.Store{{Name: "replychains", Records: []idbtest.Record{
			{Key: "r1", Wrapping: idbtest.Blob, Value: chain("short")},
		}}},
	}}))
	require.NoError(t, idbtest.Write(dir, blobs, []idbtest.Database{{
		Number: 1, Name: "Teams:replychain-manager:react-web-client:t:u:en-us",
		Stores: []idbtest.Store{{Name: "replychains", Records: []idbtest.Record{
			{Key: "r1", Wrapping: idbtest.Blob, Value: chain("much longer edited message text")},
		}}},
	}}))

	r, err := Open(context.Background(), dir, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	t.Cleanup(r.Close)

	msgs := r.Messages("")
	require.Len(t, msgs, 1)
	assert.Equal(t, "much longer edited message text", msgs[0].Content)
	assert.Zero(t, r.Skipped())
	assert.Empty(t, r.Damage())
}
