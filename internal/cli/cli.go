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
	envMaxContent = "TEAMSCACHE_MAX_CONTENT"
	envCacheTTL   = "TEAMSCACHE_CACHE_TTL"
)

type globals struct {
	leveldb    string
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
		return validate(cmd)
	}
	root.SetOut(stdout)
	root.SetErr(stderr)
	flags := root.PersistentFlags()
	flags.StringVar(&g.leveldb, "leveldb", os.Getenv(envLevelDB), "path to the IndexedDB LevelDB folder (default: auto-discovered; env "+envLevelDB+")")
	flags.StringVar(&g.account, "account", "", "only this account, as <tenantId>:<userId> (see the accounts command)")
	flags.IntVar(&g.maxContent, "max-content", envInt(envMaxContent, 600), "characters of message content in mentions, the overview and MCP results, 0 for no limit (env "+envMaxContent+")")
	flags.BoolVarP(&g.verbose, "verbose", "v", false, "log details, such as skipped records, to stderr")

	root.AddCommand(
		accountsCommand(g),
		conversationsCommand(g),
		searchCommand(g),
		readCommand(g),
		recentCommand(g),
		mentionsCommand(g),
		overviewCommand(g),
		mcpCommand(g, version),
	)
	return root
}

// run loads the cache, prints the result of query as JSON and a summary on
// stderr.
// validate rejects negative values of the numeric flags a command has.
func validate(cmd *cobra.Command) error {
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

func run[T any](g *globals, query func(*service.Snapshot) []T) error {
	snap, err := service.Load(g.ctx, g.leveldb, g.logger())
	if err != nil {
		return err
	}
	out := query(snap)
	enc := json.NewEncoder(g.stdout)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(out); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	if _, err := fmt.Fprintf(g.stderr, "# %d result(s), %d record(s) skipped\n", len(out), snap.Skipped); err != nil {
		return fmt.Errorf("write summary: %w", err)
	}
	return nil
}

const limitHelp = "maximum number of results, 0 for no limit"

func accountsCommand(g *globals) *cobra.Command {
	return &cobra.Command{
		Use:   "accounts",
		Short: "List the tenant and user contexts in the cache",
		Args:  cobra.NoArgs,
		RunE: func(*cobra.Command, []string) error {
			return run(g, func(s *service.Snapshot) []service.Account {
				out := []service.Account{}
				for _, a := range s.Accounts {
					if g.account == "" || a.Key == g.account {
						out = append(out, a)
					}
				}
				return out
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
		RunE: func(*cobra.Command, []string) error {
			return run(g, func(s *service.Snapshot) []teams.Conversation { return s.ListConversations(g.account, limit) })
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
		RunE: func(_ *cobra.Command, args []string) error {
			return run(g, func(s *service.Snapshot) []teams.Message {
				return s.Search(args[0], g.account, days, limit, g.options())
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
		RunE: func(_ *cobra.Command, args []string) error {
			return run(g, func(s *service.Snapshot) []teams.Message { return s.ReadConversation(args[0], g.account, limit) })
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
		RunE: func(*cobra.Command, []string) error {
			return run(g, func(s *service.Snapshot) []teams.Message { return s.Recent(days, g.account, limit, g.options()) })
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
		RunE: func(*cobra.Command, []string) error {
			return run(g, func(s *service.Snapshot) []service.MentionRow {
				return s.MentionsQuery(days, unread, g.account, limit, g.options())
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
		RunE: func(*cobra.Command, []string) error {
			return run(g, func(s *service.Snapshot) []service.OverviewRow { return s.Overview(days, g.account, g.options()) })
		},
	}
	cmd.Flags().IntVar(&days, "days", 7, "number of days, 0 for all")
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
			cache := service.NewCache(g.leveldb, ttl, g.logger())
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
