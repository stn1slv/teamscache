// Package service answers questions about the Teams cache: recent messages,
// mentions, an overview and searches. Both the CLI and the MCP server use it.
package service

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/stn1slv/teamscache/internal/teams"
)

// Snapshot is the parsed content of the cache.
type Snapshot struct {
	Messages      []teams.Message
	Conversations []teams.Conversation
	Mentions      []teams.Mention
	Accounts      []Account
	Skipped       int
}

// Account is a (tenant, user) context with a best-effort organization label,
// the most frequent "(Org)" suffix of sender names in its messages.
type Account struct {
	Key      string `json:"key"`
	TenantID string `json:"tenant_id"`
	UserID   string `json:"user_id"`
	Label    string `json:"label"`
}

// Load parses the cache in path, or the auto-discovered one when path is "".
// It stops with the context's error when ctx is canceled.
func Load(ctx context.Context, path string, logger *slog.Logger) (*Snapshot, error) {
	r, err := teams.Open(ctx, path, logger)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	snap := &Snapshot{
		Messages:      r.Messages(""),
		Conversations: r.Conversations(""),
		Mentions:      r.Mentions(""),
	}
	snap.Accounts = accountsWithLabels(r.Accounts(), snap.Messages)
	snap.Skipped = r.Skipped()
	return snap, nil
}

func accountsWithLabels(accounts []teams.Account, messages []teams.Message) []Account {
	type counter struct {
		counts map[string]int
		order  []string
	}
	labels := map[string]*counter{}
	for _, m := range messages {
		org, ok := teams.OrgLabel(m.Sender)
		if !ok {
			continue
		}
		c := labels[m.Account]
		if c == nil {
			c = &counter{counts: map[string]int{}}
			labels[m.Account] = c
		}
		if c.counts[org] == 0 {
			c.order = append(c.order, org)
		}
		c.counts[org]++
	}
	out := []Account{}
	seen := map[string]bool{}
	for _, a := range accounts {
		key := a.Key()
		if seen[key] {
			continue
		}
		seen[key] = true
		label := ""
		if c := labels[key]; c != nil {
			best := 0
			for _, org := range c.order { // ties go to the label seen first
				if c.counts[org] > best {
					best, label = c.counts[org], org
				}
			}
		}
		out = append(out, Account{Key: key, TenantID: a.TenantID, UserID: a.UserID, Label: label})
	}
	return out
}

// Cache keeps a parsed snapshot in memory and parses again when the cache
// folder changes or the snapshot is older than the TTL.
type Cache struct {
	path   string
	ttl    time.Duration
	logger *slog.Logger

	mu     sync.Mutex
	snap   *Snapshot
	sig    string
	loaded time.Time
}

// NewCache returns a cache for the LevelDB in path ("" to auto-discover).
func NewCache(path string, ttl time.Duration, logger *slog.Logger) *Cache {
	return &Cache{path: path, ttl: ttl, logger: logger}
}

// Snapshot returns the parsed cache, parsing it when needed.
func (c *Cache) Snapshot(ctx context.Context) (*Snapshot, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	path := c.path
	if path == "" {
		path = teams.FindCache()
	}
	sig := signature(path)
	if c.snap != nil && time.Since(c.loaded) < c.ttl && sig == c.sig {
		return c.snap, nil
	}
	snap, err := Load(ctx, path, c.logger)
	if err != nil {
		return nil, err
	}
	c.snap, c.sig, c.loaded = snap, sig, time.Now()
	return snap, nil
}

// signature is a cheap fingerprint of a folder: the newest modification time
// and the total size of its files. It changes when Teams writes new data.
func signature(dir string) string {
	if dir == "" {
		return ""
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return ""
	}
	var newest time.Time
	var total int64
	for _, e := range entries {
		info, err := e.Info()
		if err != nil {
			continue
		}
		if info.ModTime().After(newest) {
			newest = info.ModTime()
		}
		total += info.Size()
	}
	return fmt.Sprintf("%d:%d", newest.UnixNano(), total)
}

// Options holds settings shared by the queries.
type Options struct {
	// MaxContent is the number of characters of content that mentions and the
	// overview keep; 0 or less means no limit.
	MaxContent int
	// Now returns the current time; nil means time.Now. Tests replace it.
	Now func() time.Time
}

// cutoff returns the epoch milliseconds days days ago. When days is 0 or
// less, there is no cutoff and it returns minus infinity.
func (o Options) cutoff(days int) float64 {
	if days <= 0 {
		return math.Inf(-1)
	}
	now := time.Now
	if o.Now != nil {
		now = o.Now
	}
	return float64(now().UnixMilli()) - float64(days)*86400*1000
}

// EpochMillis parses a Teams timestamp: epoch milliseconds as text (like
// "1768557031836.0"), or an ISO 8601 date.
func EpochMillis(ts string) (float64, bool) {
	if ts == "" {
		return 0, false
	}
	if f, err := strconv.ParseFloat(strings.TrimSpace(ts), 64); err == nil {
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return 0, false
		}
		return f, true
	}
	iso := strings.Replace(ts, "Z", "+00:00", 1)
	for _, layout := range []string{"2006-01-02T15:04:05.999999999-07:00", "2006-01-02 15:04:05.999999999-07:00"} {
		if t, err := time.Parse(layout, iso); err == nil {
			return float64(t.UnixNano()) / 1e6, true
		}
	}
	// A time without a zone is local time.
	for _, layout := range []string{"2006-01-02T15:04:05.999999999", "2006-01-02 15:04:05.999999999", "2006-01-02"} {
		if t, err := time.ParseInLocation(layout, ts, time.Local); err == nil {
			return float64(t.UnixNano()) / 1e6, true
		}
	}
	return 0, false
}

// timed is an item with its parsed timestamp, so that sorting parses each
// timestamp once.
type timed[T any] struct {
	item T
	e    float64
	ok   bool // the timestamp could be parsed
}

func withTime[T any](item T, ts string) timed[T] {
	e, ok := EpochMillis(ts)
	return timed[T]{item: item, e: e, ok: ok}
}

// newestFirst sorts items by time, newest first, with undated items last.
func newestFirst[T any](items []timed[T]) {
	sort.SliceStable(items, func(i, j int) bool {
		if items[i].ok != items[j].ok {
			return items[i].ok
		}
		return items[i].e > items[j].e
	})
}

func untimed[T any](items []timed[T]) []T {
	out := make([]T, 0, len(items))
	for _, t := range items {
		out = append(out, t.item)
	}
	return out
}

// firstChars returns the first n characters of s.
func firstChars(s string, n int) string {
	if n <= 0 || utf8.RuneCountInString(s) <= n {
		return s
	}
	return string([]rune(s)[:n])
}

// Truncate shortens s to n characters and marks the cut with an ellipsis. A
// limit of 0 or less means no limit.
func Truncate(s string, n int) string {
	if n <= 0 || utf8.RuneCountInString(s) <= n {
		return s
	}
	return firstChars(s, n) + "…"
}

// limited returns at most limit items; a limit of 0 or less means no limit.
func limited[T any](items []T, limit int) []T {
	if limit > 0 && len(items) > limit {
		return items[:limit]
	}
	return items
}

// ListConversations returns chats and channels that have a title or a last
// message.
func (s *Snapshot) ListConversations(account string, limit int) []teams.Conversation {
	out := []teams.Conversation{}
	for _, c := range s.Conversations {
		if account != "" && c.Account != account {
			continue
		}
		if c.Title != "" || c.LastMessage != "" {
			out = append(out, c)
		}
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out
}

// ReadConversation returns the last limit messages of a conversation, oldest
// first.
func (s *Snapshot) ReadConversation(conversationID, account string, limit int) []teams.Message {
	var found []timed[teams.Message]
	for _, m := range s.Messages {
		if m.ConversationID == conversationID && (account == "" || m.Account == account) {
			found = append(found, withTime(m, m.Timestamp))
		}
	}
	// Oldest first; undated messages count as time 0 and come first.
	sort.SliceStable(found, func(i, j int) bool { return found[i].e < found[j].e })
	msgs := untimed(found)
	if limit > 0 && len(msgs) > limit {
		msgs = msgs[len(msgs)-limit:]
	}
	return msgs
}

// Search returns messages whose content or sender contains query, ignoring
// case, newest first. When days is above 0, only messages of the last days
// days count.
func (s *Snapshot) Search(query, account string, days, limit int, opts Options) []teams.Message {
	q := strings.ToLower(query)
	cutoff := opts.cutoff(days)
	var hits []timed[teams.Message]
	for _, m := range s.Messages {
		if account != "" && m.Account != account {
			continue
		}
		t := withTime(m, m.Timestamp)
		if days > 0 && (!t.ok || t.e < cutoff) {
			continue
		}
		if strings.Contains(strings.ToLower(m.Content), q) || strings.Contains(strings.ToLower(m.Sender), q) {
			hits = append(hits, t)
		}
	}
	newestFirst(hits)
	return untimed(limited(hits, limit))
}

// Recent returns the messages of the last days days, newest first. When days
// is 0 or less, all messages count, and undated ones come last.
func (s *Snapshot) Recent(days int, account string, limit int, opts Options) []teams.Message {
	cutoff := opts.cutoff(days)
	var hits []timed[teams.Message]
	for _, m := range s.Messages {
		if account != "" && m.Account != account {
			continue
		}
		t := withTime(m, m.Timestamp)
		if days > 0 && (!t.ok || t.e < cutoff) {
			continue
		}
		hits = append(hits, t)
	}
	newestFirst(hits)
	return untimed(limited(hits, limit))
}

// MentionRow is an @-mention with the content of the message.
type MentionRow struct {
	Timestamp      string `json:"timestamp"`
	IsRead         bool   `json:"is_read"`
	Conversation   string `json:"conversation"`
	ConversationID string `json:"conversation_id"`
	Sender         string `json:"sender"`
	Content        string `json:"content"`
	Account        string `json:"account"`
}

type accountKey struct{ account, id string }

func (s *Snapshot) titles() map[accountKey]string {
	out := map[accountKey]string{}
	for _, c := range s.Conversations {
		out[accountKey{c.Account, c.ID}] = c.Title
	}
	return out
}

// messageIndex finds the newest version of a message.
type messageIndex struct {
	byConversation map[[3]string]teams.Message
	byID           map[accountKey]teams.Message
}

func newestMessages(msgs []teams.Message) messageIndex {
	idx := messageIndex{byConversation: map[[3]string]teams.Message{}, byID: map[accountKey]teams.Message{}}
	for _, m := range msgs {
		k3 := [3]string{m.Account, m.ConversationID, m.MessageID}
		if old, ok := idx.byConversation[k3]; !ok || m.Seq >= old.Seq {
			idx.byConversation[k3] = m
		}
		k := accountKey{m.Account, m.MessageID}
		if old, ok := idx.byID[k]; !ok || m.Seq >= old.Seq {
			idx.byID[k] = m
		}
	}
	return idx
}

// find returns the message in the conversation, or else a message with the
// same id in the account.
func (idx messageIndex) find(account, conversationID, messageID string) (teams.Message, bool) {
	if m, ok := idx.byConversation[[3]string{account, conversationID, messageID}]; ok {
		return m, true
	}
	m, ok := idx.byID[accountKey{account, messageID}]
	return m, ok
}

// MentionsQuery returns @-mentions of the user, newest first. When days is
// above 0, only mentions of the last days days count.
func (s *Snapshot) MentionsQuery(days int, unreadOnly bool, account string, limit int, opts Options) []MentionRow {
	content := newestMessages(s.Messages)
	titles := s.titles()
	cutoff := opts.cutoff(days)
	var rows []timed[MentionRow]
	for _, mt := range s.Mentions {
		if account != "" && mt.Account != account {
			continue
		}
		if unreadOnly && mt.IsRead {
			continue
		}
		e, ok := EpochMillis(mt.Timestamp)
		if days > 0 && (!ok || e < cutoff) {
			continue
		}
		row := MentionRow{
			Timestamp: mt.Timestamp, IsRead: mt.IsRead, ConversationID: mt.ConversationID, Account: mt.Account,
			Conversation: titles[accountKey{mt.Account, mt.ConversationID}],
		}
		if row.Conversation == "" {
			row.Conversation = mt.ConversationID
		}
		if m, ok := content.find(mt.Account, mt.ConversationID, mt.MessageID); ok {
			row.Sender = m.Sender
			row.Content = firstChars(m.Content, opts.MaxContent)
		}
		rows = append(rows, timed[MentionRow]{item: row, e: e, ok: ok})
	}
	newestFirst(rows)
	return untimed(limited(rows, limit))
}

// OverviewRow is the recent activity of one conversation.
type OverviewRow struct {
	Conversation   string   `json:"conversation"`
	ConversationID string   `json:"conversation_id"`
	Count          int      `json:"count"`
	Senders        []string `json:"senders"`
	LastSender     string   `json:"last_sender"`
	LastMessage    string   `json:"last_message"`
	LastTime       string   `json:"last_time"`
	Account        string   `json:"account"`
}

// Overview groups the messages of the last days days by conversation, newest
// conversation first. When days is 0 or less, all messages count.
func (s *Snapshot) Overview(days int, account string, opts Options) []OverviewRow {
	const maxSenders = 8
	titles := s.titles()
	cutoff := opts.cutoff(days)
	type group struct {
		key     accountKey
		count   int
		senders map[string]bool
		lastE   float64
		lastOK  bool
		last    *teams.Message
	}
	var groups []*group
	byKey := map[accountKey]*group{}
	for i := range s.Messages {
		m := &s.Messages[i]
		if account != "" && m.Account != account {
			continue
		}
		e, ok := EpochMillis(m.Timestamp)
		if days > 0 && (!ok || e < cutoff) {
			continue
		}
		k := accountKey{m.Account, m.ConversationID}
		g := byKey[k]
		if g == nil {
			g = &group{key: k, senders: map[string]bool{}}
			byKey[k] = g
			groups = append(groups, g)
		}
		g.count++
		if m.Sender != "" {
			g.senders[m.Sender] = true
		}
		// The newest dated message is the last one; on equal times, the
		// newer record version wins.
		newer := g.last == nil ||
			(ok && !g.lastOK) ||
			(ok == g.lastOK && (e > g.lastE || (e == g.lastE && m.Seq > g.last.Seq)))
		if newer {
			g.lastE, g.lastOK, g.last = e, ok, m
		}
	}
	rows := make([]timed[OverviewRow], 0, len(groups))
	for _, g := range groups {
		senders := make([]string, 0, len(g.senders))
		for name := range g.senders {
			senders = append(senders, name)
		}
		sort.Strings(senders)
		row := OverviewRow{
			Conversation: titles[g.key], ConversationID: g.key.id, Count: g.count,
			Senders: limited(senders, maxSenders), Account: g.key.account,
		}
		if row.Conversation == "" {
			row.Conversation = g.key.id
		}
		if g.last != nil {
			row.LastSender, row.LastMessage, row.LastTime = g.last.Sender, firstChars(g.last.Content, opts.MaxContent), g.last.Timestamp
		}
		rows = append(rows, timed[OverviewRow]{item: row, e: g.lastE, ok: g.lastOK})
	}
	newestFirst(rows)
	return untimed(rows)
}

// MessageRow is a message in the compact form the MCP tools return.
type MessageRow struct {
	Sender         string `json:"sender"`
	Timestamp      string `json:"timestamp"`
	Content        string `json:"content"`
	ConversationID string `json:"conversation_id"`
	Account        string `json:"account"`
}

// Rows converts messages to compact rows with content cut to maxContent
// characters.
func Rows(msgs []teams.Message, maxContent int) []MessageRow {
	out := make([]MessageRow, 0, len(msgs))
	for _, m := range msgs {
		out = append(out, MessageRow{
			Sender: m.Sender, Timestamp: m.Timestamp, Content: Truncate(m.Content, maxContent),
			ConversationID: m.ConversationID, Account: m.Account,
		})
	}
	return out
}
