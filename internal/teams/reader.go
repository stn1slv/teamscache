// Package teams reads the new Microsoft Teams (v2) message cache from the local
// disk, read-only.
//
// The Teams client keeps recent conversations in a Chromium IndexedDB backed
// by LevelDB. Each (tenant, user) context has its own databases, named
// "Teams:<manager>:react-web-client:<tenantId>:<userObjectId>:<locale>":
//   - messages: "Teams:replychain-manager:..." store "replychains" and its
//     numbered copies such as "replychains-2", each record
//     has a "messageMap" of message id to message,
//   - chats and channels: "Teams:conversation-manager:..." store "conversations",
//   - @-mentions: "Teams:messaging-slice-manager:..." store
//     "mentions-metadata-items".
package teams

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"iter"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"

	"github.com/stn1slv/teamscache/internal/indexeddb"
	"github.com/stn1slv/teamscache/internal/leveldb"
	"github.com/stn1slv/teamscache/internal/v8"
)

// ErrCacheNotFound reports that no Teams cache folder was found.
var ErrCacheNotFound = errors.New("teams cache not found")

// ErrBlobTooLarge reports a blob that exceeds the decoding size limit.
var ErrBlobTooLarge = errors.New("blob exceeds the size limit")

type sourceReadError struct{ err error }

func (e *sourceReadError) Error() string { return "read cache source: " + e.err.Error() }
func (e *sourceReadError) Unwrap() error { return e.err }

type sourceReader struct{ reader io.Reader }

func (r sourceReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	if err != nil && !errors.Is(err, io.EOF) {
		return n, &sourceReadError{err: err}
	}
	return n, err
}

// Account is one (tenant, user) context in the cache.
type Account struct {
	TenantID string
	UserID   string
}

// Key returns "<tenantId>:<userId>", the value of the account filters.
func (a Account) Key() string { return a.TenantID + ":" + a.UserID }

// Profile holds account metadata for the signed-in user from the profiles store.
type Profile struct {
	Account     string
	TenantName  string
	DisplayName string
	Email       string
}

// Message is one cached message.
type Message struct {
	Account             string `json:"account"`
	ConversationID      string `json:"conversation_id"`
	ReplyChainID        string `json:"reply_chain_id"`
	MessageID           string `json:"message_id"`
	Sender              string `json:"sender"`
	Creator             string `json:"creator,omitempty"`
	IsSentByCurrentUser *bool  `json:"is_sent_by_current_user,omitempty"`
	Timestamp           string `json:"timestamp"`
	Content             string `json:"content"`
	ContentType         string `json:"content_type"`
	// Seq is the LevelDB sequence number of the record version; a higher
	// number is a newer version.
	Seq uint64 `json:"-"`
}

// Conversation is a chat or channel.
type Conversation struct {
	Account         string `json:"account"`
	ID              string `json:"id"`
	Title           string `json:"title"`
	Type            string `json:"type"`
	LastMessageTime string `json:"last_message_time"`
	LastMessage     string `json:"last_message"`
}

// Mention is an @-mention of the user. It has no content; join it with a
// Message on (Account, MessageID).
type Mention struct {
	Account        string `json:"account"`
	ConversationID string `json:"conversation_id"`
	MessageID      string `json:"message_id"`
	ReplyChainID   string `json:"reply_chain_id"`
	Timestamp      string `json:"timestamp"`
	IsRead         bool   `json:"is_read"`
}

// Reader reads a Teams cache.
type Reader struct {
	db         *indexeddb.DB
	tmp        string
	sourcePath string
	blobScope  Scope
	skipped    int
	damage     []leveldb.Damage
	logger     *slog.Logger
}

// Scope selects which Teams stores Open copies external blobs for.
type Scope uint8

// Store scopes.
const (
	ScopeMessages Scope = 1 << iota
	ScopeConversations
	ScopeMentions
	ScopeProfiles
	ScopeAll = ScopeMessages | ScopeConversations | ScopeMentions | ScopeProfiles
)

// Open reads the Teams LevelDB in path, or the auto-discovered one when path
// is empty. The folder is first copied to a temporary folder, because the
// running Teams client holds a lock on it. Call Close to remove the copy.
// When ctx is canceled, Open removes the copy and returns the context's error.
func Open(ctx context.Context, path string, logger *slog.Logger) (*Reader, error) {
	return OpenFor(ctx, path, logger, ScopeAll)
}

// OpenFor works like Open, but copies external blobs only for the stores in scope.
func OpenFor(ctx context.Context, path string, logger *slog.Logger, scope Scope) (*Reader, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if path == "" {
		path = FindCache()
	}
	if info, err := os.Stat(path); path == "" || err != nil || !info.IsDir() {
		return nil, fmt.Errorf("%w: pass a LevelDB path, or sign in to the new Teams client once; looked in %v",
			ErrCacheNotFound, DefaultCacheGlobs())
	}
	// Keep this prefix: local tooling may watch for it.
	tmp, err := os.MkdirTemp("", "msteams-cache-")
	if err != nil {
		return nil, fmt.Errorf("create temporary folder: %w", err)
	}
	r := &Reader{tmp: tmp, sourcePath: path, logger: logger}
	copyDir := filepath.Join(tmp, "leveldb")
	stable, err := copyDataFiles(ctx, path, copyDir, logger)
	if err != nil {
		r.Close()
		return nil, err
	}
	copyBlobDir := filepath.Join(tmp, "blobs")
	r.db, err = indexeddb.Open(ctx, copyDir, copyBlobDir, logger)
	if err != nil {
		r.Close()
		return nil, err
	}
	r.damage = r.db.Damage()
	if !stable {
		r.damage = append(r.damage, leveldb.Damage{
			File:   filepath.Base(filepath.Clean(path)),
			Reason: "cache changed during copy; result can miss recent data",
		})
	}
	if err := r.EnsureBlobs(ctx, scope); err != nil {
		r.Close()
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		r.Close()
		return nil, err
	}
	return r, nil
}

// EnsureBlobs copies external blobs needed by any newly requested stores in scope.
func (r *Reader) EnsureBlobs(ctx context.Context, scope Scope) error {
	missing := scope &^ r.blobScope
	if missing == 0 {
		return ctx.Err()
	}
	blobAccept := r.blobFilter(missing)
	if blobAccept == nil {
		r.blobScope |= missing
		return ctx.Err()
	}
	copyBlobDir := filepath.Join(r.tmp, "blobs")
	for _, file := range r.db.BlobFilesFor(blobAccept) {
		dst := filepath.Join(copyBlobDir, file)
		if err := os.MkdirAll(filepath.Dir(dst), 0o700); err != nil {
			return fmt.Errorf("create blob copy folder: %w", err)
		}
		if err := copyFile(ctx, filepath.Join(blobDirFor(r.sourcePath), file), dst, leveldb.MaxDecodedSize); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			var sourceErr *sourceReadError
			if errors.As(err, &sourceErr) || errors.Is(err, ErrBlobTooLarge) {
				reason := err.Error()
				if errors.Is(err, fs.ErrNotExist) {
					reason = "referenced blob is missing"
				}
				r.logger.Warn("could not copy a referenced blob", "file", file, "error", err)
				r.damage = append(r.damage, leveldb.Damage{File: file, Reason: reason})
				continue
			}
			return err
		}
	}
	r.blobScope |= missing
	return ctx.Err()
}

func (r *Reader) blobFilter(scope Scope) func(dbNumber, storeID uint64) bool {
	type dbStore struct{ db, store uint64 }
	allowed := map[dbStore]bool{}
	add := func(manager, store string) {
		for _, d := range r.db.Databases() {
			if !strings.Contains(d.Name, "Teams:"+manager+":") || len(strings.Split(d.Name, ":")) < 6 {
				continue
			}
			for _, id := range r.db.ObjectStoreIDs(d.Number, func(name string) bool { return isStore(name, store) }) {
				allowed[dbStore{db: d.Number, store: id}] = true
			}
		}
	}
	if scope&ScopeMessages != 0 {
		add("replychain-manager", "replychains")
	}
	if scope&ScopeConversations != 0 {
		add("conversation-manager", "conversations")
	}
	if scope&ScopeMentions != 0 {
		add("messaging-slice-manager", "mentions-metadata-items")
	}
	if scope&ScopeProfiles != 0 {
		add("profiles", "profiles")
	}
	if len(allowed) == 0 {
		return nil
	}
	return func(dbNumber, storeID uint64) bool {
		return allowed[dbStore{db: dbNumber, store: storeID}]
	}
}

// Close removes the temporary copy.
func (r *Reader) Close() {
	if r.tmp != "" {
		if err := os.RemoveAll(r.tmp); err != nil {
			r.logger.Warn("could not remove the temporary copy", "path", r.tmp, "error", err)
		}
		r.tmp = ""
	}
}

// Skipped is the number of records that could not be decoded.
func (r *Reader) Skipped() int { return r.skipped }

// Damage returns the file sections or blob files missing from the snapshot.
func (r *Reader) Damage() []leveldb.Damage { return r.damage }

// copyAttempts is how many times the copy is tried while Teams changes the
// set of files, for example during a compaction.
const copyAttempts = 3

// copyDataFiles copies the LevelDB table and log files of src to dst. Other
// files are not needed, and the LOCK file can be locked by the running client.
// When the set of files changes during the copy, the copy starts again.
func copyDataFiles(ctx context.Context, src, dst string, logger *slog.Logger) (bool, error) {
	for attempt := 1; ; attempt++ {
		stable, err := copyOnce(ctx, src, dst)
		if err != nil || stable {
			return stable, err
		}
		if attempt == copyAttempts {
			logger.Warn("the cache changed during each copy; the result can miss recent data", "attempts", attempt)
			return false, nil
		}
		if err := os.RemoveAll(dst); err != nil {
			return false, fmt.Errorf("remove incomplete copy: %w", err)
		}
	}
}

// copyOnce copies the data files once. It reports whether the set of files
// was the same before and after the copy and no file disappeared.
func copyOnce(ctx context.Context, src, dst string) (stable bool, err error) {
	before, err := dataFiles(src)
	if err != nil {
		return false, err
	}
	if err := os.Mkdir(dst, 0o700); err != nil {
		return false, fmt.Errorf("create copy folder: %w", err)
	}
	stable = true
	for _, name := range before {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		err := copyFile(ctx, filepath.Join(src, name), filepath.Join(dst, name), 0)
		if errors.Is(err, fs.ErrNotExist) {
			stable = false
			continue
		}
		if err != nil {
			return false, err
		}
	}
	after, err := dataFiles(src)
	if err != nil {
		return false, err
	}
	return stable && slices.Equal(before, after), nil
}

// dataFiles returns the sorted names of the LevelDB data files in dir.
func dataFiles(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read cache folder: %w", err)
	}
	var names []string
	for _, e := range entries {
		if e.Type().IsRegular() && leveldb.IsDataFile(e.Name()) {
			names = append(names, e.Name())
		}
	}
	return names, nil
}

// copyFile copies a file. A positive limit caps the bytes written, including
// when the source grows during copying. Source failures remove incomplete copies.
func copyFile(ctx context.Context, src, dst string, limit int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	in, err := os.Open(src)
	if err != nil {
		return &sourceReadError{err: err}
	}
	defer func() { _ = in.Close() }() // read-only, a close error changes nothing
	if limit > 0 {
		info, err := in.Stat()
		if err != nil {
			return &sourceReadError{err: err}
		}
		if info.Size() > limit {
			return fmt.Errorf("%w: maximum %d bytes", ErrBlobTooLarge, limit)
		}
	}
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("copy cache file: %w", err)
	}
	reader := leveldb.ContextReader{Context: ctx, Reader: sourceReader{reader: in}}
	var copied int64
	if limit > 0 {
		copied, err = io.Copy(out, io.LimitReader(reader, limit))
		if err == nil && copied == limit {
			var extra [1]byte
			n, readErr := reader.Read(extra[:])
			if n > 0 {
				err = fmt.Errorf("%w: maximum %d bytes", ErrBlobTooLarge, limit)
			} else if readErr != nil && !errors.Is(readErr, io.EOF) {
				err = readErr
			}
		}
	} else {
		_, err = io.Copy(out, reader)
	}
	if err != nil {
		_ = out.Close() // the copy error is the one to report
		if removeErr := os.Remove(dst); removeErr != nil {
			return fmt.Errorf("remove incomplete copy: %w", removeErr)
		}
		return fmt.Errorf("copy cache file: %w", err)
	}
	if err := out.Close(); err != nil {
		return fmt.Errorf("copy cache file: %w", err)
	}
	return nil
}

// storeRecord is one version of an object store record.
type storeRecord struct {
	key    string
	seq    uint64
	live   bool
	value  any
	failed bool   // the value could not be decoded
	store  uint64 // the object store id, when known
}

// storeFunc yields, for each context of a manager in database order, the
// account and every record version of one object store and its numbered
// copies, each tagged with its store.
type storeFunc func(manager, store string) iter.Seq2[Account, iter.Seq[storeRecord]]

type storeKey struct {
	store uint64
	key   string
}

// latest keeps, in each store, the newest version of each record, and drops a
// record whose newest version in that store is a deletion. A deletion counts
// only in its own store: this keeps records that Teams moved to another
// store, but an older live copy can come back after a real deletion. A
// version that could not be decoded is kept, so it still hides older
// versions. Records keep the order of their first version.
func latest(records iter.Seq[storeRecord]) []storeRecord {
	var order []storeKey
	newest := map[storeKey]storeRecord{}
	for r := range records {
		k := storeKey{store: r.store, key: r.key}
		old, seen := newest[k]
		if !seen {
			order = append(order, k)
		}
		if !seen || r.seq >= old.seq {
			newest[k] = r
		}
	}
	out := make([]storeRecord, 0, len(order))
	for _, k := range order {
		if r := newest[k]; r.live {
			out = append(out, r)
		}
	}
	return out
}

// current keeps the newest version of each record across stores, as chosen by
// latest, and drops it when it could not be decoded; an older version is never
// shown in place of a newer one. Records keep the order of their first
// version.
func current(records iter.Seq[storeRecord]) []storeRecord {
	var order []string
	newest := map[string]storeRecord{}
	for _, r := range latest(records) {
		old, seen := newest[r.key]
		if !seen {
			order = append(order, r.key)
		}
		if !seen || r.seq >= old.seq {
			newest[r.key] = r
		}
	}
	out := make([]storeRecord, 0, len(order))
	for _, k := range order {
		if r := newest[k]; !r.failed && r.value != nil {
			out = append(out, r)
		}
	}
	return out
}

// isStore reports whether name is store or a numbered copy of it, such as
// "replychains-2". Teams also writes records to such stores, and some
// messages are only there.
func isStore(name, store string) bool {
	if name == store {
		return true
	}
	suffix, ok := strings.CutPrefix(name, store+"-")
	if !ok || suffix == "" {
		return false
	}
	for i := range len(suffix) {
		if suffix[i] < '0' || suffix[i] > '9' {
			return false
		}
	}
	return true
}

// records yields every record version of one object store.
func (r *Reader) records(dbNumber, storeID uint64) iter.Seq[storeRecord] {
	return func(yield func(storeRecord) bool) {
		for rec := range r.db.Records(dbNumber, storeID, r.onBad) {
			if !yield(storeRecord{key: string(rec.Key), seq: rec.Seq, live: rec.Live, value: rec.Value, failed: rec.Err != nil, store: storeID}) {
				return
			}
		}
	}
}

// stores yields, for each database of manager, the account and the record
// versions of store and of its numbered copies, in store id order.
func (r *Reader) stores(manager, store string) iter.Seq2[Account, iter.Seq[storeRecord]] {
	return func(yield func(Account, iter.Seq[storeRecord]) bool) {
		for _, d := range r.db.Databases() {
			if !strings.Contains(d.Name, "Teams:"+manager+":") {
				continue
			}
			// Teams:<manager>:react-web-client:<tenantId>:<userObjectId>:<locale>
			parts := strings.Split(d.Name, ":")
			if len(parts) < 6 {
				r.logger.Debug("skipped a database with an unexpected name", "name", d.Name)
				continue
			}
			acc := Account{TenantID: parts[len(parts)-3], UserID: parts[len(parts)-2]}
			storeIDs := r.db.ObjectStoreIDs(d.Number, func(name string) bool { return isStore(name, store) })
			if len(storeIDs) == 0 {
				continue
			}
			values := func(yieldRecord func(storeRecord) bool) {
				for _, storeID := range storeIDs {
					for rec := range r.records(d.Number, storeID) {
						if !yieldRecord(rec) {
							return
						}
					}
				}
			}
			if !yield(acc, values) {
				return
			}
		}
	}
}

func (r *Reader) onBad(err error) {
	r.skipped++
	r.logger.Debug("skipped a record", "error", err)
}

// Accounts returns the contexts that have a message store.
func (r *Reader) Accounts() []Account {
	return accounts(r.stores)
}

// Messages returns the messages of the newest version of each reply chain in
// each store, optionally only those of one account key. A message found in
// several stores is returned once, from the chain version with the highest
// sequence number; it is left out when that version has no text. A chain
// version that could not be decoded hides the older versions of that chain.
func (r *Reader) Messages(account string) []Message {
	return messages(r.stores, account)
}

// Conversations returns chat and channel records, newest version only.
func (r *Reader) Conversations(account string) []Conversation {
	return conversations(r.stores, account)
}

// Mentions returns @-mention entries, newest version only, so the read state
// is the current one. It is the only read state the cache keeps; general read
// state is not stored locally.
func (r *Reader) Mentions(account string) []Mention {
	return mentions(r.stores, account)
}

// Profiles returns the signed-in user's profile per account from the profiles store.
func (r *Reader) Profiles(account string) []Profile {
	return profiles(r.stores, account)
}

func accounts(stores storeFunc) []Account {
	var out []Account
	for acc := range stores("replychain-manager", "replychains") {
		out = append(out, acc)
	}
	return out
}

func profiles(stores storeFunc, account string) []Profile {
	type matched struct {
		profile    Profile
		tenantSeq  uint64
		displaySeq uint64
		emailSeq   uint64
	}
	best := map[string]matched{}
	var order []string
	for acc, values := range stores("profiles", "profiles") {
		key := acc.Key()
		if account != "" && key != account {
			continue
		}
		for _, rec := range current(values) {
			v, ok := rec.value.(*v8.Object)
			if !ok {
				continue
			}
			oid := plainText(get(v, "objectId"))
			mri := plainText(get(v, "mri"))
			if !matchesUserID(acc.UserID, oid, mri) {
				continue
			}
			tenantName := plainText(get(v, "tenantName"))
			displayName := plainText(get(v, "displayName"))
			email := plainText(or(get(v, "email"), or(get(v, "mail"), get(v, "userPrincipalName"))))
			if tenantName == "" && displayName == "" && email == "" {
				continue
			}
			m, seen := best[key]
			if !seen {
				order = append(order, key)
				m.profile.Account = key
			}
			if tenantName != "" && (m.profile.TenantName == "" || rec.seq >= m.tenantSeq) {
				m.profile.TenantName, m.tenantSeq = tenantName, rec.seq
			}
			if displayName != "" && (m.profile.DisplayName == "" || rec.seq >= m.displaySeq) {
				m.profile.DisplayName, m.displaySeq = displayName, rec.seq
			}
			if email != "" && (m.profile.Email == "" || rec.seq >= m.emailSeq) {
				m.profile.Email, m.emailSeq = email, rec.seq
			}
			best[key] = m
		}
	}
	out := make([]Profile, 0, len(order))
	for _, key := range order {
		out = append(out, best[key].profile)
	}
	return out
}

func matchesUserID(userID, objectID, mri string) bool {
	if userID == "" {
		return false
	}
	if strings.EqualFold(objectID, userID) || strings.EqualFold(mri, userID) {
		return true
	}
	return len(mri) > len(userID)+1 && mri[len(mri)-len(userID)-1] == ':' &&
		strings.EqualFold(mri[len(mri)-len(userID):], userID)
}

func messages(stores storeFunc, account string) []Message {
	type messageKey struct{ account, conversation, id, entry string }
	var found []Message
	index := map[messageKey]int{}
	for acc, values := range stores("replychain-manager", "replychains") {
		if account != "" && acc.Key() != account {
			continue
		}
		// Each store's copy of a chain can hold other messages, so the
		// copies are merged by message id rather than by chain.
		versions := latest(values)
		hiddenBelow := map[string]uint64{}
		for _, v := range versions {
			if v.failed && v.seq > hiddenBelow[v.key] {
				hiddenBelow[v.key] = v.seq
			}
		}
		for _, version := range versions {
			if version.failed || version.seq < hiddenBelow[version.key] {
				continue
			}
			rec, ok := version.value.(*v8.Object)
			if !ok {
				continue
			}
			mm, ok := or(get(rec, "messageMap"), nil).(*v8.Object)
			if !ok {
				continue
			}
			for i, mid := range mm.Keys() {
				msg, ok := mm.Values()[i].(*v8.Object)
				if !ok {
					continue
				}
				var sentByMe *bool
				if b, ok := get(msg, "isSentByCurrentUser").(bool); ok {
					sentByMe = &b
				}
				m := Message{
					Account:             acc.Key(),
					ConversationID:      valueString(getOr(rec, "conversationId", "")),
					ReplyChainID:        valueString(getOr(rec, "replyChainId", "")),
					MessageID:           valueString(getOr(msg, "id", mid)),
					Sender:              text(or(get(msg, "imDisplayName"), get(msg, "from"))),
					Creator:             plainText(or(get(msg, "creator"), get(msg, "from"))),
					IsSentByCurrentUser: sentByMe,
					Timestamp:           valueString(or(or(get(msg, "originalArrivalTime"), get(msg, "composetime")), "")),
					Content:             messageText(msg),
					ContentType:         valueString(or(get(msg, "messageType"), "")),
					Seq:                 version.seq,
				}
				k := messageKey{account: m.Account, conversation: m.ConversationID, id: m.MessageID}
				if m.MessageID == "" { // without an id, only the same entry of the same chain is the same message
					k.entry = version.key + "\x00" + valueString(mid)
				}
				// The newest version wins before the text is checked, so an
				// emptied message does not show its older text.
				if i, seen := index[k]; !seen {
					index[k] = len(found)
					found = append(found, m)
				} else if m.Seq >= found[i].Seq {
					found[i] = m
				}
			}
		}
	}
	var out []Message
	for _, m := range found {
		if m.Content != "" {
			out = append(out, m)
		}
	}
	return out
}

func conversations(stores storeFunc, account string) []Conversation {
	var out []Conversation
	for acc, values := range stores("conversation-manager", "conversations") {
		if account != "" && acc.Key() != account {
			continue
		}
		for _, rec := range current(values) {
			v, ok := rec.value.(*v8.Object)
			if !ok {
				continue
			}
			last, _ := get(v, "lastMessage").(*v8.Object)
			lastText := ""
			if last != nil && last.Len() > 0 {
				lastText = messageText(last)
			}
			out = append(out, Conversation{
				Account:         acc.Key(),
				ID:              valueString(getOr(v, "id", "")),
				Title:           conversationTitle(v),
				Type:            valueString(getOr(v, "type", "")),
				LastMessageTime: valueString(getOr(v, "lastMessageTimeUtc", "")),
				LastMessage:     lastText,
			})
		}
	}
	return out
}

// conversationTitle returns the display title of a conversation record, or
// "". Channels and named group chats have threadProperties.topic. Other chats
// use chatTitle, the title the Teams client computed; a short title with an
// unknown member gives way to the long title.
func conversationTitle(record *v8.Object) string {
	if topic := text(or(get(get(record, "threadProperties"), "topic"), "")); topic != "" {
		return topic
	}
	chat, ok := get(record, "chatTitle").(*v8.Object)
	if !ok {
		return ""
	}
	short := ""
	if !truthy(get(chat, "hasUnknownUserInShortTitle")) {
		short = text(or(get(chat, "shortTitle"), ""))
	}
	if short != "" {
		return short
	}
	return text(or(get(chat, "longTitle"), ""))
}

func mentions(stores storeFunc, account string) []Mention {
	var out []Mention
	for acc, values := range stores("messaging-slice-manager", "mentions-metadata-items") {
		if account != "" && acc.Key() != account {
			continue
		}
		for _, rec := range current(values) {
			v, ok := rec.value.(*v8.Object)
			if !ok {
				continue
			}
			isRead, _ := get(v, "isRead").(bool)
			out = append(out, Mention{
				Account:        acc.Key(),
				ConversationID: valueString(getOr(v, "sourceThreadId", "")),
				MessageID:      valueString(getOr(v, "sourceMessageId", "")),
				ReplyChainID:   valueString(getOr(v, "sourceReplyChainId", "")),
				Timestamp:      valueString(getOr(v, "timestamp", "")),
				IsRead:         isRead,
			})
		}
	}
	return out
}

var orgRE = regexp.MustCompile(`\(([^)]+)\)\s*$`)

// OrgLabel returns the organization in a display name like "Ann Lee (Org)".
func OrgLabel(sender string) (string, bool) {
	m := orgRE.FindStringSubmatch(sender)
	if m == nil {
		return "", false
	}
	return m[1], true
}
