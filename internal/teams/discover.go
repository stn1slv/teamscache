package teams

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"
)

const (
	leveldbName   = "https_teams.microsoft.com_0.indexeddb.leveldb"
	leveldbGlob   = "*.indexeddb.leveldb"
	leveldbSuffix = ".indexeddb.leveldb"
)

// teamsOriginHosts are the known Microsoft Teams hostnames (and their
// subdomains) whose IndexedDB folders hold Teams caches.
var teamsOriginHosts = []string{
	"teams.microsoft.com",
	"teams.cloud.microsoft",
	"teams.live.com",
	"teams.microsoft.us",
	"teams.microsoftonline.cn",
}

// isTeamsOriginDir reports whether a folder name under IndexedDB belongs to a
// known Microsoft Teams origin.
func isTeamsOriginDir(name string) bool {
	stem, ok := strings.CutSuffix(strings.ToLower(name), leveldbSuffix)
	if !ok {
		return false
	}
	if rest, ok := strings.CutPrefix(stem, "https_"); ok {
		stem = rest
	} else if rest, ok := strings.CutPrefix(stem, "http_"); ok {
		stem = rest
	} else {
		return false
	}
	idx := strings.LastIndexByte(stem, '_')
	if idx <= 0 || idx == len(stem)-1 {
		return false
	}
	for i := idx + 1; i < len(stem); i++ {
		if stem[i] < '0' || stem[i] > '9' {
			return false
		}
	}
	host := stem[:idx]
	for _, allowed := range teamsOriginHosts {
		if host == allowed || strings.HasSuffix(host, "."+allowed) {
			return true
		}
	}
	return false
}

// DiscoveredCache is one Teams IndexedDB folder found on this computer.
type DiscoveredCache struct {
	Profile  string    `json:"profile"`
	Path     string    `json:"path"`
	Modified time.Time `json:"modified"`
}

// DefaultCacheGlobs returns the places where the new Teams client keeps its
// IndexedDB LevelDB on this operating system.
func DefaultCacheGlobs() []string {
	home, _ := os.UserHomeDir()
	switch runtime.GOOS {
	case "darwin":
		base := filepath.Join(home, "Library", "Containers", "com.microsoft.teams2", "Data", "Library",
			"Application Support", "Microsoft", "MSTeams", "EBWebView")
		return []string{filepath.Join(base, "*", "IndexedDB", leveldbGlob)}
	case "windows":
		local := os.Getenv("LOCALAPPDATA")
		if local == "" {
			local = filepath.Join(home, "AppData", "Local")
		}
		return []string{filepath.Join(local, "Packages", "MSTeams_*", "LocalCache", "Microsoft", "MSTeams",
			"EBWebView", "*", "IndexedDB", leveldbGlob)}
	default:
		// Unofficial Linux clients, best effort.
		return []string{filepath.Join(home, ".config", "*eams*", "*", "IndexedDB", leveldbGlob)}
	}
}

// DiscoverCaches returns all discovered Teams LevelDB folders, sorted from
// newest modification time to oldest.
func DiscoverCaches() []DiscoveredCache {
	var found []DiscoveredCache
	for _, pattern := range DefaultCacheGlobs() {
		hits, _ := filepath.Glob(pattern)
		sort.Strings(hits)
		for _, hit := range hits {
			if !isTeamsOriginDir(filepath.Base(hit)) {
				continue
			}
			changed, ok := lastChange(hit)
			if !ok {
				continue
			}
			found = append(found, DiscoveredCache{
				Profile:  profileName(hit),
				Path:     hit,
				Modified: changed,
			})
		}
	}
	sort.SliceStable(found, func(i, j int) bool {
		return found[i].Modified.After(found[j].Modified)
	})
	return found
}

func profileName(leveldbDir string) string {
	return filepath.Base(filepath.Dir(filepath.Dir(filepath.Clean(leveldbDir))))
}

// FindCache returns the Teams LevelDB folder whose files changed last, or ""
// when there is none. A client can have more than one WebView2 profile, and
// the one in use is the one Teams writes to.
func FindCache() string {
	caches := DiscoverCaches()
	if len(caches) == 0 {
		return ""
	}
	return caches[0].Path
}

// FindCacheFor returns the Teams LevelDB folder for the named WebView2 profile.
func FindCacheFor(profile string) (string, error) {
	return matchProfile(DiscoverCaches(), profile)
}

func matchProfile(caches []DiscoveredCache, profile string) (string, error) {
	names := make([]string, 0, len(caches))
	for _, c := range caches {
		names = append(names, c.Profile)
		if c.Profile == profile {
			return c.Path, nil
		}
	}
	for _, c := range caches {
		if strings.EqualFold(c.Profile, profile) {
			return c.Path, nil
		}
	}
	return "", fmt.Errorf("%w: profile %q not found (available: %v)", ErrCacheNotFound, profile, names)
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
