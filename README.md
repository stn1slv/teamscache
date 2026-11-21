# teamscache

`teamscache` reads the message cache of the new Microsoft Teams (v2) desktop client on your computer. It prints chats, messages and @-mentions as JSON, and it can also run as a read-only MCP server for AI assistants.

It does not use Microsoft Graph, OAuth, an Azure app registration or the network. It reads only what the signed-in Teams client already keeps on your disk.

## Why

To read Teams messages through Microsoft Graph, you need permissions such as `Chat.Read`, and many tenants allow them only after admin consent. As a guest in another company's tenant, you often cannot get that consent. The desktop client still keeps your recent conversations on your disk, and `teamscache` reads them from there.

## Install

With Homebrew on macOS or Linux:

```bash
brew install stn1slv/tap/teamscache
```

Binaries for macOS, Linux and Windows are on the [releases page](https://github.com/stn1slv/teamscache/releases).

With Go 1.27 or later:

```bash
go install github.com/stn1slv/teamscache/cmd/teamscache@latest
```

Or build from source with `make build`; the binary is written to `bin/teamscache`.

You need a new Teams client that has been signed in at least once. Auto-discovery of the cache works on macOS and Windows. On Linux it is best effort.

## Usage

```bash
teamscache accounts                      # tenant and user contexts in the cache
teamscache profiles                      # discovered WebView2 cache profiles
teamscache overview --days 7             # recent activity grouped by conversation
teamscache recent --days 3               # messages of the last 3 days, newest first
teamscache mentions --unread             # @-mentions of you that are not read yet
teamscache search "quarterly budget"     # text in message content or sender name, newest first
teamscache conversations                 # chats and channels
teamscache read "19:abc@thread.v2"       # last messages of one conversation
teamscache dump                          # accounts, conversations, messages and mentions in one JSON object
```

All commands print JSON to stdout and a short summary to stderr, for example `# 12 result(s), 0 record(s) skipped, 0 damaged section(s)`. Skipped records could not be decoded. Damaged sections are unreadable file sections or missing referenced blob files, and are counted separately.

| Flag | Commands | Meaning |
|------|----------|---------|
| `--account <tenantId>:<userId>` | all | Only one account. The keys come from `teamscache accounts`. With `mcp`, every tool serves only this account. |
| `--limit N` | most | Maximum number of results; `0` means no limit. |
| `--days N` | `search`, `recent`, `mentions`, `overview`, `dump` | Only items of the last N days; `0` means all. |
| `--leveldb PATH` | all | Read this cache folder instead of the auto-discovered one. |
| `--profile NAME` | all | Choose a discovered WebView2 profile by folder name (see `teamscache profiles`). |
| `--max-content N` | all | Characters of message content in mentions, the overview, `dump` and MCP results; `0` means no limit (default 600). |
| `-v`, `--verbose` | all | Log details, such as skipped records, to stderr. |

Environment variables:

| Variable | Meaning |
|----------|---------|
| `TEAMSCACHE_LEVELDB` | Default for `--leveldb`. |
| `TEAMSCACHE_PROFILE` | Default for `--profile`. |
| `TEAMSCACHE_MAX_CONTENT` | Default for `--max-content`. |
| `TEAMSCACHE_CACHE_TTL` | Seconds the MCP server reuses a parsed cache (default 180). |

The cache has no general read or unread marker. The read state of @-mentions is the only read state it keeps, so `mentions --unread` is the reliable way to find what needs your attention.

## MCP server

`teamscache mcp` runs a read-only MCP server on stdin and stdout. Example configuration for Claude Desktop or Claude Code:

```json
{
  "mcpServers": {
    "teams": { "command": "teamscache", "args": ["mcp"] }
  }
}
```

| Tool | Purpose |
|------|---------|
| `overview` | Who messaged you: recent activity grouped by conversation. A good first call. |
| `recent_messages` | Messages of the last N days, newest first. |
| `mentions` | @-mentions of you, with content and read state; `unread_only` filter. |
| `unread_messages` | Your unread @-mentions. |
| `search_messages` | Text search in content and sender names, with an optional `days` filter. |
| `list_conversations` | Chats and channels with their titles. |
| `read_conversation` | Last messages of one conversation. |
| `list_accounts` | Accounts in the cache; use the `key` as the `account` filter. |

The server parses the cache on the first tool call and keeps the result in memory. It parses again when the TTL ends or when Teams writes new data.

Every tool response keeps its rows in `result` and includes `skipped_records`, `damaged_sections`, and `partial`. When `partial` is true, some cache data could not be read, so an empty result does not confirm that no matching messages exist.

## How it works

The new Teams client (`com.microsoft.teams2` on macOS, the `MSTeams` package on Windows) is an Edge WebView2 app. It stores recent conversations in an IndexedDB database, which Chromium keeps in LevelDB with values in the V8 serialization format. `teamscache`:

1. finds the LevelDB folder, or uses `--leveldb`,
2. copies its data files and referenced blob files to a temporary folder, because the running client locks the files, and removes the copy when it is done,
3. reads the LevelDB files directly (tables and logs, including Snappy-compressed blocks), without a LevelDB engine,
4. decodes the IndexedDB keys and the Blink and V8 values,
5. maps the Teams schema: messages in `replychains` and its numbered copies such as `replychains-2`, chats in `conversations`, mentions in `mentions-metadata-items`, one database set per tenant and user.

LevelDB can keep older versions of a record until it compacts its files. `teamscache` uses only the newest version of each record, so an edited message shows its current text and the read state of a mention is the current one.

The LevelDB, IndexedDB and V8 decoding is a Go port of parts of [ccl_chromium_reader](https://github.com/cclgroupltd/ccl_chromium_reader). See [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).

## Privacy

The tool is read-only and makes no network calls. The temporary copy of the cache is created with owner-only permissions and removed after each run. Output can contain message content from all tenants in the cache, so treat it as confidential.

## Development

```bash
make setup    # download dependencies
make test     # unit tests with the race detector
make lint     # golangci-lint and gofumpt
make format   # format the code
make build    # build bin/teamscache
make upgrade-deps # update dependencies and pinned Go tools
```

Tests use synthetic data only. The test fixtures and their generators are in `testdata` folders:

- `internal/v8/testdata`: values written by Node.js `v8.serialize` (`gen.mjs`) and the result of `ccl_chromium_reader` for the same bytes (`golden.py`).
- `internal/indexeddb/testdata`: a synthetic IndexedDB written by `gen/main.go`, and the records `ccl_chromium_reader` reads from it (`gen/golden.py`).
- `internal/teams/testdata/gen`: a synthetic Teams cache with edge cases, for manual end-to-end checks.

## License

MIT. See [LICENSE](LICENSE) and [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).
