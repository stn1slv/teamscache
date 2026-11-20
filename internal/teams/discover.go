package teams

import (
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"
)

const leveldbName = "https_teams.microsoft.com_0.indexeddb.leveldb"

// DefaultCacheGlobs returns the places where the new Teams client keeps its
// IndexedDB LevelDB on this operating system.
func DefaultCacheGlobs() []string {
	home, _ := os.UserHomeDir()
	switch runtime.GOOS {
	case "darwin":
		base := filepath.Join(home, "Library", "Containers", "com.microsoft.teams2", "Data", "Library",
			"Application Support", "Microsoft", "MSTeams", "EBWebView")
		return []string{filepath.Join(base, "*", "IndexedDB", leveldbName)}
	case "windows":
		local := os.Getenv("LOCALAPPDATA")
		if local == "" {
			local = filepath.Join(home, "AppData", "Local")
		}
		return []string{filepath.Join(local, "Packages", "MSTeams_*", "LocalCache", "Microsoft", "MSTeams",
			"EBWebView", "*", "IndexedDB", leveldbName)}
	default:
		// Unofficial Linux clients, best effort.
		return []string{filepath.Join(home, ".config", "*eams*", "*", "IndexedDB", leveldbName)}
	}
}

// FindCache returns the Teams LevelDB folder whose files changed last, or ""
// when there is none. A client can have more than one WebView2 profile, and
// the one in use is the one Teams writes to.
func FindCache() string {
	best := ""
	var bestTime time.Time
	for _, pattern := range DefaultCacheGlobs() {
		hits, _ := filepath.Glob(pattern)
		sort.Strings(hits)
		for _, hit := range hits {
			changed, ok := lastChange(hit)
			if !ok {
				continue
			}
			if best == "" || changed.After(bestTime) {
				best, bestTime = hit, changed
			}
		}
	}
	return best
}

// lastChange returns the newest modification time of dir and its files.
// Appending to a file does not change the time of its folder.
func lastChange(dir string) (time.Time, bool) {
	info, err := os.Stat(dir)
	if err != nil || !info.IsDir() {
		return time.Time{}, false
	}
	newest := info.ModTime()
	entries, err := os.ReadDir(dir)
	if err != nil {
		return newest, true
	}
	for _, e := range entries {
		if fi, err := e.Info(); err == nil && fi.ModTime().After(newest) {
			newest = fi.ModTime()
		}
	}
	return newest, true
}

// blobDirFor returns the folder Chromium uses for large values of a LevelDB
// folder: the same name with ".blob" in place of ".leveldb".
func blobDirFor(leveldbDir string) string {
	return strings.TrimSuffix(filepath.Clean(leveldbDir), ".leveldb") + ".blob"
}
