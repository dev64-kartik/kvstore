// Package vfs is the filesystem seam that storage code (WAL, snapshots,
// recovery) goes through, so crash tests can swap the real disk for one that
// simulates power loss.
//
// The interface lives here rather than in each consumer because three
// packages (wal, store, localdb) share it along with its test implementation,
// as with io/fs in the standard library.
package vfs

import (
	"io"
	"os"
)

// FS is the set of filesystem operations storage code is allowed to use.
//
// Durability contract (what a crash may lose):
//   - File data is durable only after File.Sync.
//   - A name (from Create, Rename, Remove) is durable only after SyncDir on
//     its parent directory. A synced file whose name was never made durable
//     may vanish on crash.
//
// Errors for a missing or already-existing file satisfy
// errors.Is(err, fs.ErrNotExist) and errors.Is(err, fs.ErrExist).
type FS interface {
	// Create creates a new, empty file for reading and writing. It fails if
	// the file exists, so a temp file can never start out holding stale bytes.
	Create(name string) (File, error)
	// Open opens an existing file read-only.
	Open(name string) (File, error)
	// OpenAppend opens an existing file for reading and appending. Writes
	// always go to the current end of the file, including after Truncate.
	OpenAppend(name string) (File, error)
	// Rename atomically renames a file. Open handles to it keep working.
	Rename(oldname, newname string) error
	// Remove deletes a file.
	Remove(name string) error
	// List returns the names (not paths) of the entries in dir, sorted.
	List(dir string) ([]string, error)
	// SyncDir makes every create, rename and remove in dir durable.
	SyncDir(dir string) error
}

// File is an open file. *os.File satisfies it.
type File interface {
	io.Reader
	io.Writer
	Sync() error
	Truncate(size int64) error
	Close() error
}

// ReadFile reads the whole named file through fsys.
func ReadFile(fsys FS, name string) (_ []byte, err error) {
	f, err := fsys.Open(name)
	if err != nil {
		return nil, err
	}
	defer func() {
		if cerr := f.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()
	return io.ReadAll(f)
}

// OS returns an FS backed by the real operating system.
//
// On macOS, Go's File.Sync issues F_FULLFSYNC rather than fsync: plain fsync
// on macOS doesn't flush the drive's write cache. It is correct, and slow.
func OS() FS {
	return osFS{}
}

type osFS struct{}

func (osFS) Create(name string) (File, error) {
	return wrap(os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644))
}

func (osFS) Open(name string) (File, error) {
	return wrap(os.Open(name))
}

func (osFS) OpenAppend(name string) (File, error) {
	return wrap(os.OpenFile(name, os.O_RDWR|os.O_APPEND, 0))
}

func (osFS) Rename(oldname, newname string) error {
	return os.Rename(oldname, newname)
}

func (osFS) Remove(name string) error {
	return os.Remove(name)
}

func (osFS) List(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir) // already sorted by name
	if err != nil {
		return nil, err
	}
	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name()
	}
	return names, nil
}

// SyncDir fsyncs the directory itself, which is what persists its entries
// (file names). Fsyncing a file never does that.
func (osFS) SyncDir(dir string) (err error) {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := d.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()
	return d.Sync()
}

// wrap converts os.OpenFile's (*os.File, error) into (File, error).
//
// It must return a literal nil on error. Returning the nil *os.File directly
// would produce a non-nil File interface holding a nil pointer, so a caller's
// `f != nil` check would pass, and f.Close() would then fail.
func wrap(f *os.File, err error) (File, error) {
	if err != nil {
		return nil, err
	}
	return f, nil
}
