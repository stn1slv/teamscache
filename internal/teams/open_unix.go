//go:build unix

package teams

import (
	"errors"
	"os"
	"syscall"
)

// openRegularNoFollow opens path without following symlinks and without
// blocking on a FIFO.
func openRegularNoFollow(path string) (*os.File, error) {
	// #nosec G304 -- caller validates path before calling openRegularNoFollow.
	f, err := os.OpenFile(path, syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		if errors.Is(err, syscall.ELOOP) {
			return nil, errNonRegularFile
		}
		return nil, err
	}
	return f, nil
}
