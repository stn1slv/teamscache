//go:build !unix

package teams

import "os"

// openRegularNoFollow opens path read-only on non-Unix platforms. Callers still
// verify IsRegular and SameFile on the opened descriptor.
func openRegularNoFollow(path string) (*os.File, error) {
	// #nosec G304 -- caller validates path before calling openRegularNoFollow.
	return os.Open(path)
}
