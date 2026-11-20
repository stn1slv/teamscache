// Command gen writes a synthetic Teams cache with edge cases, for manual
// end-to-end checks of the CLI and the MCP server.
//
//	go run ./internal/teams/testdata/gen OUT_DIR/sample.leveldb
package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/stn1slv/teamscache/internal/idbtest"
)

type obj = idbtest.Obj

func p(k string, v any) idbtest.Prop { return idbtest.Prop{Key: k, Value: v} }

func name(manager, account string) string {
	return "Teams:" + manager + ":react-web-client:" + account + ":en-us"
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: gen OUT_DIR/sample.leveldb")
		os.Exit(2)
	}
	dir := os.Args[1]
	msgs := obj{
		p("m1", obj{p("id", "m1"), p("content", "<p>Budget &amp; plan</p>"), p("imDisplayName", "Ann Lee (Org A)"), p("originalArrivalTime", 1768557031836.0), p("messageType", "RichText/Html")}),
		p("m2", obj{p("content", "no id, compose time"), p("imDisplayName", idbtest.Undefined{}), p("from", "8:orgid:abc"), p("composetime", "2026-01-16T09:50:31.836Z")}),
		p("m3", obj{p("id", 42), p("content", "int id"), p("imDisplayName", "Bob (Org B)"), p("originalArrivalTime", 1768557031900.0), p("messageType", nil)}),
		p("m4", obj{p("content", "   ")}),
		p("m5", "not an object"),
		p("m6", obj{p("content", "two-byte ✓ 😀 text"), p("imDisplayName", "Cid (Org A)"), p("originalArrivalTime", 1768557032000.0)}),
	}
	err := idbtest.Write(dir, filepath.Join(filepath.Dir(dir), "unused-blobs"), []idbtest.Database{
		{Number: 1, Name: name("replychain-manager", "t1:u1"), Stores: []idbtest.Store{{Name: "replychains", Records: []idbtest.Record{
			{Key: "r1", Value: obj{p("conversationId", "c1"), p("replyChainId", 1768557031836.0), p("messageMap", msgs)}},
			{Key: "r1", Value: obj{p("conversationId", "c1"), p("replyChainId", "r1"), p("messageMap", obj{p("m1", obj{p("id", "m1"), p("content", "edited")})})}},
			{Key: "r2", Value: obj{p("conversationId", idbtest.Undefined{}), p("messageMap", obj{})}},
			{Key: "r3", Value: []any{"not", "an object"}},
			{Key: "bad", Value: "x", Wrapping: idbtest.Corrupt},
			{Key: "gone", Value: obj{p("conversationId", "c9"), p("messageMap", obj{p("z", obj{p("content", "deleted later")})})}, Deleted: true},
			{Key: "zip", Value: obj{p("conversationId", "c2"), p("messageMap", obj{p("s", obj{p("content", "compressed"), p("imDisplayName", "Dan (Org B)")})})}, Wrapping: idbtest.Snappy},
		}}}},
		{Number: 2, Name: name("conversation-manager", "t1:u1"), Stores: []idbtest.Store{{Name: "conversations", Records: []idbtest.Record{
			{Key: "c1", Value: obj{
				p("id", "c1"), p("type", "Chat"), p("threadProperties", obj{p("topic", nil)}),
				p("chatTitle", obj{p("shortTitle", "Unknown"), p("longTitle", "Ann Lee, Bob Ray"), p("hasUnknownUserInShortTitle", true)}),
				p("lastMessageTimeUtc", 1768557031836.0), p("lastMessage", obj{p("content", "<b>last</b>")}),
			}},
			{Key: "c2", Value: obj{p("id", "c2"), p("threadProperties", obj{p("topic", "Channel &lt;1&gt;")})}},
			{Key: "c3", Value: obj{p("id", "c3")}},
		}}}},
		{Number: 3, Name: name("replychain-manager", "t2:u2"), Stores: []idbtest.Store{{Name: "replychains", Records: []idbtest.Record{
			{Key: "q", Value: obj{p("conversationId", "c5"), p("messageMap", obj{p("q1", obj{p("content", "budget in tenant two"), p("imDisplayName", "Eve")})})}},
		}}}},
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
