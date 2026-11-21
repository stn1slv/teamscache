// Package cli is the command-line interface of teamscache.
package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/spf13/cobra"

	"github.com/stn1slv/teamscache/internal/mcpserver"
	"github.com/stn1slv/teamscache/internal/service"
	"github.com/stn1slv/teamscache/internal/teams"
)

// ErrInvalidFlag reports a flag value the command does not accept.
var ErrInvalidFlag = errors.New("invalid flag")

// Environment variables.
const (
	envLevelDB    = "TEAMSCACHE_LEVELDB"
	envProfile    = "TEAMSCACHE_PROFILE"
	envMaxContent = "TEAMSCACHE_MAX_CONTENT"
	envCacheTTL   = "TEAMSCACHE_CACHE_TTL"
)

type globals struct {
	leveldb    string
	profile    string
	account    string
	maxContent int
	verbose    bool
	ctx        context.Context
	stdout     io.Writer
	stderr     io.Writer
}

func (g *globals) logger() *slog.Logger {
	level := slog.LevelWarn
	if g.verbose {
		level = slog.LevelDebug
	}
	return slog.New(slog.NewJSONHandler(g.stderr, &slog.HandlerOptions{Level: level}))
}

func (g *globals) options() service.Options {
	return service.Options{MaxContent: g.maxContent, Now: time.Now}
}

func envInt(name string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(name)); err == nil {
		return v
	}
	return def
}

// NewRootCommand returns the teamscache command with its subcommands.
func NewRootCommand(version string, stdout, stderr io.Writer) *cobra.Command {
	g := &globals{stdout: stdout, stderr: stderr}
	root := &cobra.Command{
		Use:   "teamscache",
		Short: "Read the local Microsoft Teams (v2) message cache",
		Long: "teamscache reads the message cache that the new Microsoft Teams desktop client keeps on this computer.\n" +
			"It needs no Microsoft Graph access, no sign-in and no network. All commands are read-only and print JSON.\n\n" +
			"The cache folder is found automatically. Use --leveldb or " + envLevelDB + " to choose another one.",
		Version:       version,
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	root.PersistentPreRunE = func(cmd *cobra.Command, _ []string) error {
		g.ctx = cmd.Context()
		return validate(cmd, g)
	}
	root.SetOut(stdout)
	root.SetErr(stderr)
	flags := root.PersistentFlags()
	flags.StringVar(&g.leveldb, "leveldb", os.Getenv(envLevelDB), "path to the IndexedDB LevelDB folder (default: auto-discovered; env "+envLevelDB+")")
	flags.StringVar(&g.profile, "profile", os.Getenv(envProfile), "WebView2 profile folder name when multiple Teams caches exist (env "+envProfile+")")
	flags.StringVar(&g.account, "account", "", "only this account, as <tenantId>:<userId> (see the accounts command)")
	flags.IntVar(&g.maxContent, "max-content", envInt(envMaxContent, 600), "characters of message content in mentions, the overview and MCP results, 0 for no limit (env "+envMaxContent+")")
	flags.BoolVarP(&g.verbose, "verbose", "v", false, "log details, such as skipped records, to stderr")

	root.AddCommand(
		accountsCommand(g),
		profilesCommand(g),
		conversationsCommand(g),
		searchCommand(g),
		readCommand(g),
		recentCommand(g),
		mentionsCommand(g),
		overviewCommand(g),
		dumpCommand(g),
		mcpCommand(g, version),
	)
	return root
}

// validate rejects invalid flag combinations, conflicting env settings, and
// negative numeric flags.
func validate(cmd *cobra.Command, g *globals) error {
	leveldbFlag := cmd.Flags().Changed("leveldb")
	profileFlag := cmd.Flags().Changed("profile")
	if leveldbFlag && profileFlag {
		return fmt.Errorf("%w: --leveldb and --profile cannot be used together", ErrInvalidFlag)
	}
	if !leveldbFlag && !profileFlag && g.leveldb != "" && g.profile != "" {
		return fmt.Errorf("%w: %s and %s cannot both be set", ErrInvalidFlag, envLevelDB, envProfile)
	}
	for _, name := range []string{"max-content", "limit", "days", "cache-ttl"} {
		f := cmd.Flags().Lookup(name)
		if f == nil {
			continue
		}
		if strings.HasPrefix(f.Value.String(), "-") {
			return fmt.Errorf("%w: --%s must be 0 or more, got %s", ErrInvalidFlag, name, f.Value.String())
		}
	}
	return nil
}

func (g *globals) cachePath(cmd *cobra.Command) (string, error) {
	if cmd.Flags().Changed("profile") {
		return teams.FindCacheFor(g.profile)
	}
	if cmd.Flags().Changed("leveldb") || g.leveldb != "" {
		return g.leveldb, nil
	}
	if g.profile != "" {
		return teams.FindCacheFor(g.profile)
	}
	return "", nil
}

func encodeJSON(w io.Writer, out any) error {
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(out); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	return nil
}

func writeJSON(g *globals, snap *service.Snapshot, count int, out any) error {
	if err := encodeJSON(g.stdout, out); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(g.stderr, "# %d result(s), %d record(s) skipped, %d damaged section(s)\n", count, snap.Skipped, len(snap.Damage)); err != nil {
		return fmt.Errorf("write summary: %w", err)
	}
	return nil
}

func profilesCommand(g *globals) *cobra.Command {
	return &cobra.Command{
		Use:   "profiles",
		Short: "List discovered Teams WebView2 cache profiles, newest first",
		Args:  cobra.NoArgs,
		RunE: func(*cobra.Command, []string) error {
			caches := teams.DiscoverCaches()
			if caches == nil {
				caches = []teams.DiscoveredCache{}
			}
			if err := encodeJSON(g.stdout, caches); err != nil {
				return err
			}
			_, err := fmt.Fprintf(g.stderr, "# %d profile(s)\n", len(caches))
			return err
		},
	}
}

func run[T any](cmd *cobra.Command, g *globals, scope teams.Scope, query func(*service.Snapshot) ([]T, error)) error {
	path, err := g.cachePath(cmd)
	if err != nil {
		return err
	}
	snap, err := service.LoadFor(g.ctx, path, g.logger(), scope)
	if err != nil {
		return err
	}
	out, err := query(snap)
	if err != nil {
		return err
	}
	return writeJSON(g, snap, len(out), out)
}

const limitHelp = "maximum number of results, 0 for no limit"

func accountsCommand(g *globals) *cobra.Command {
	return &cobra.Command{
		Use:   "accounts",
		Short: "List the tenant and user contexts in the cache",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return run(cmd, g, teams.ScopeProfiles, func(s *service.Snapshot) ([]service.Account, error) {
				out := []service.Account{}
				for _, a := range s.Accounts {
					if err := g.ctx.Err(); err != nil {
						return nil, err
					}
					if g.account == "" || a.Key == g.account {
						out = append(out, a)
					}
				}
				return out, g.ctx.Err()
			})
		},
	}
}

func conversationsCommand(g *globals) *cobra.Command {
	var limit int
	cmd := &cobra.Command{
		Use:   "conversations",
		Short: "List chats and channels",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return run(cmd, g, teams.ScopeConversations, func(s *service.Snapshot) ([]teams.Conversation, error) {
				return s.ListConversations(g.ctx, g.account, limit)
			})
		},
	}
	cmd.Flags().IntVar(&limit, "limit", 50, limitHelp)
	return cmd
}

func searchCommand(g *globals) *cobra.Command {
	var limit, days int
	cmd := &cobra.Command{
		Use:   "search QUERY",
		Short: "Find messages whose content or sender contains QUERY, ignoring case",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return run(cmd, g, teams.ScopeMessages, func(s *service.Snapshot) ([]teams.Message, error) {
				return s.Search(g.ctx, args[0], g.account, days, limit, g.options())
			})
		},
	}
	cmd.Flags().IntVar(&limit, "limit", 50, limitHelp)
	cmd.Flags().IntVar(&days, "days", 0, "only messages of the last N days, 0 for all")
	return cmd
}

func readCommand(g *globals) *cobra.Command {
	var limit int
	cmd := &cobra.Command{
		Use:   "read CONVERSATION_ID",
		Short: "Show the most recent messages of a conversation, oldest first",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return run(cmd, g, teams.ScopeMessages, func(s *service.Snapshot) ([]teams.Message, error) {
				return s.ReadConversation(g.ctx, args[0], g.account, limit)
			})
		},
	}
	cmd.Flags().IntVar(&limit, "limit", 50, limitHelp)
	return cmd
}

func recentCommand(g *globals) *cobra.Command {
	var limit, days int
	cmd := &cobra.Command{
		Use:   "recent",
		Short: "Show messages of the last days, newest first",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return run(cmd, g, teams.ScopeMessages, func(s *service.Snapshot) ([]teams.Message, error) {
				return s.Recent(g.ctx, days, g.account, limit, g.options())
			})
		},
	}
	cmd.Flags().IntVar(&limit, "limit", 200, limitHelp)
	cmd.Flags().IntVar(&days, "days", 7, "number of days, 0 for all")
	return cmd
}

func mentionsCommand(g *globals) *cobra.Command {
	var limit, days int
	var unread bool
	cmd := &cobra.Command{
		Use:   "mentions",
		Short: "Show @-mentions of you, newest first, with content",
		Long: "Show @-mentions of you, newest first, with content.\n" +
			"The read state of mentions is the only read state the local cache keeps.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return run(cmd, g, teams.ScopeMessages|teams.ScopeConversations|teams.ScopeMentions, func(s *service.Snapshot) ([]service.MentionRow, error) {
				return s.MentionsQuery(g.ctx, days, unread, g.account, limit, g.options())
			})
		},
	}
	cmd.Flags().IntVar(&limit, "limit", 100, limitHelp)
	cmd.Flags().IntVar(&days, "days", 0, "only mentions of the last N days, 0 for all")
	cmd.Flags().BoolVar(&unread, "unread", false, "only mentions not read yet")
	return cmd
}

func overviewCommand(g *globals) *cobra.Command {
	var days int
	cmd := &cobra.Command{
		Use:   "overview",
		Short: "Show recent activity grouped by conversation, newest first",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return run(cmd, g, teams.ScopeMessages|teams.ScopeConversations, func(s *service.Snapshot) ([]service.OverviewRow, error) {
				return s.Overview(g.ctx, days, g.account, g.options())
			})
		},
	}
	cmd.Flags().IntVar(&days, "days", 7, "number of days, 0 for all")
	return cmd
}

func dumpCommand(g *globals) *cobra.Command {
	var days int
	cmd := &cobra.Command{
		Use:   "dump",
		Short: "Export accounts, conversations, messages, and mentions in one JSON object",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			path, err := g.cachePath(cmd)
			if err != nil {
				return err
			}
			snap, err := service.LoadFor(g.ctx, path, g.logger(), teams.ScopeAll)
			if err != nil {
				return err
			}
			out, err := snap.Dump(g.ctx, days, g.account, g.options())
			if err != nil {
				return err
			}
			count := len(out.Accounts) + len(out.Conversations) + len(out.Messages) + len(out.Mentions)
			return writeJSON(g, snap, count, out)
		},
	}
	cmd.Flags().IntVar(&days, "days", 0, "only messages and mentions of the last N days, 0 for all")
	return cmd
}

func mcpCommand(g *globals, version string) *cobra.Command {
	var ttl time.Duration
	cmd := &cobra.Command{
		Use:   "mcp",
		Short: "Run the read-only MCP server on stdin and stdout",
		Long: "Run the read-only MCP server on stdin and stdout.\n" +
			"The cache is parsed on the first tool call and kept in memory until the TTL ends or Teams writes new data.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			path, err := g.cachePath(cmd)
			if err != nil {
				return err
			}
			cache := service.NewCache(path, ttl, g.logger())
			server := mcpserver.New(cache, g.options(), version, g.account)
			return server.Run(cmd.Context(), &mcp.StdioTransport{})
		},
	}
	defTTL := time.Duration(envInt(envCacheTTL, 180)) * time.Second
	cmd.Flags().DurationVar(&ttl, "cache-ttl", defTTL, "how long a parsed cache is reused (env "+envCacheTTL+", in seconds)")
	return cmd
}

// Execute runs the command line and returns the exit code.
func Execute(ctx context.Context, version string, args []string) int {
	root := NewRootCommand(version, os.Stdout, os.Stderr)
	root.SetArgs(args)
	if err := root.ExecuteContext(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		return 1
	}
	return 0
}
