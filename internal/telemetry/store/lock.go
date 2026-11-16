package store

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

// lockFileName is a dedicated file at the data-directory root whose open
// descriptor carries the exclusive flock. It is never read, renamed, or
// removed; its presence on disk means nothing without a live process holding
// the lock, so it survives restarts harmlessly.
const lockFileName = "LOCK"

// dataDirLock is an exclusive, advisory, whole-process lock on a telemetry data
// directory. A live store deletes, renames, and rewrites files under this root
// with no cross-process coordination, so a second process on the same directory
// would delete the first's staging and compaction work mid-write. The lock
// serializes those processes: the second acquirer fails fast instead of racing.
type dataDirLock struct {
	file *os.File
}

// acquireDataDirLock takes an exclusive, non-blocking flock on root/LOCK. When
// another process already holds it the call fails immediately with an error
// naming the directory, rather than blocking until the other process exits.
func acquireDataDirLock(root string) (*dataDirLock, error) {
	path := filepath.Join(root, lockFileName)
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open telemetry data directory lock %s: %w", path, err)
	}
	if err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = file.Close()
		if errors.Is(err, unix.EWOULDBLOCK) {
			return nil, fmt.Errorf("telemetry data directory %s is held by another process; stop the running Pitot (or repair) using this directory before starting here", root)
		}
		return nil, fmt.Errorf("lock telemetry data directory %s: %w", root, err)
	}
	return &dataDirLock{file: file}, nil
}

// Close drops the flock and closes the descriptor. Closing alone releases the
// lock; the explicit unlock makes the release independent of close ordering.
func (l *dataDirLock) Close() error {
	if l == nil || l.file == nil {
		return nil
	}
	unlockErr := unix.Flock(int(l.file.Fd()), unix.LOCK_UN)
	closeErr := l.file.Close()
	l.file = nil
	return errors.Join(unlockErr, closeErr)
}
