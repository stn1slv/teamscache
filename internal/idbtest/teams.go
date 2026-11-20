package idbtest

import (
	"path/filepath"
	"strings"
	"time"
)

// SampleAccount is the account key of the sample Teams cache.
const SampleAccount = "tenant-1:user-1"

// WriteTeamsSample writes a small Teams cache to dir: one account with two
// messages in the chat "c1" (titled "Project"), sent one hour and one day
// before now, and an unread mention of the newer message.
func WriteTeamsSample(dir string, now time.Time) error {
	p := func(k string, v any) Prop { return Prop{Key: k, Value: v} }
	at := func(ago time.Duration) float64 { return float64(now.Add(-ago).UnixMilli()) }
	name := func(manager string) string {
		return "Teams:" + manager + ":react-web-client:" + SampleAccount + ":en-us"
	}
	return Write(dir, strings.TrimSuffix(dir, filepath.Ext(dir))+".blob", []Database{
		{Number: 1, Name: name("replychain-manager"), Stores: []Store{{Name: "replychains", Records: []Record{
			{Key: "r1", Value: Obj{
				p("conversationId", "c1"), p("replyChainId", "r1"),
				p("messageMap", Obj{
					p("m1", Obj{p("id", "m1"), p("content", "<p>Budget &lt;draft&gt; is ready</p>"), p("imDisplayName", "Ann Lee (Org)"), p("originalArrivalTime", at(time.Hour)), p("messageType", "RichText/Html")}),
					p("m0", Obj{p("id", "m0"), p("content", "Hello"), p("imDisplayName", "Bob Ray"), p("originalArrivalTime", at(24*time.Hour)), p("messageType", "Text")}),
				}),
			}},
		}}}},
		{Number: 2, Name: name("conversation-manager"), Stores: []Store{{Name: "conversations", Records: []Record{
			{Key: "c1", Value: Obj{p("id", "c1"), p("type", "Chat"), p("threadProperties", Obj{p("topic", "Project")})}},
		}}}},
		{Number: 3, Name: name("messaging-slice-manager"), Stores: []Store{{Name: "mentions-metadata-items", Records: []Record{
			{Key: "x1", Value: Obj{p("sourceThreadId", "c1"), p("sourceMessageId", "m1"), p("sourceReplyChainId", "r1"), p("timestamp", at(time.Hour)), p("isRead", false)}},
		}}}},
	})
}
