// Package crashfs is an in-memory vfs.FS that simulates power loss. It tracks,
// for every file and directory, what has been made durable and what lives only
// in the "page cache". Crash then rebuilds the filesystem as a disk might look
// after the machine loses power.
//
// It exists for tests: a SIGKILL test can't catch a missing fsync, because the
// real kernel's page cache survives process death.
package crashfs

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"sync"

	"github.com/dev64-kartik/kvstore/internal/vfs"
)

// ErrCrashed is returned by every operation on a File opened before a Crash,
// as if the process that held it had died.
var ErrCrashed = errors.New("crashfs: file handle from before a crash")

// Hook is called before every operation, with the op name ("create", "open",
// "openappend", "rename", "remove", "list", "syncdir", "read", "write",
// "sync", "truncate", "close") and the path involved. A non-nil return fails
// the operation with that error, and nothing changes. The hook runs without
// the FS lock held, so it may call Crash itself.
type Hook func(op, name string) error

// FS is a crash-simulating filesystem. It is safe for concurrent use.
type FS struct {
	mu   sync.Mutex
	hook Hook
	dirs map[string]*dir
	gen  int // bumped by Crash; handles from an older generation are dead
}

type inode struct {
	data    []byte // what reads see now
	durable []byte // contents at the last successful Sync
}

type dir struct {
	current map[string]*inode // names as reads see them now
	durable map[string]*inode // names as of the last SyncDir
	pending []dirOp           // name changes since the last SyncDir, in order
}

// dirOp is one name change. Create sets ino; rename sets from.
type dirOp struct {
	kind string // "create", "remove" or "rename"
	name string
	from string
	ino  *inode
}

var _ vfs.FS = (*FS)(nil) // compile-time check that FS implements vfs.FS

// New returns an empty FS with no directories.
func New() *FS {
	return &FS{dirs: map[string]*dir{}}
}

// Mkdir creates an empty directory and makes it durable immediately. It is a
// test-setup helper and is not part of vfs.FS.
func (f *FS) Mkdir(path string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.dirs[filepath.Clean(path)] = &dir{current: map[string]*inode{}, durable: map[string]*inode{}}
}

// SetHook installs h (nil removes it).
func (f *FS) SetHook(h Hook) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.hook = h
}

func (f *FS) callHook(op, name string) error {
	f.mu.Lock()
	h := f.hook
	f.mu.Unlock()
	if h == nil {
		return nil
	}
	return h(op, name)
}

// lookup splits a path into its directory and base name.
func (f *FS) lookup(op, name string) (*dir, string, error) {
	name = filepath.Clean(name)
	d, ok := f.dirs[filepath.Dir(name)]
	if !ok {
		return nil, "", &fs.PathError{Op: op, Path: name, Err: fs.ErrNotExist}
	}
	return d, filepath.Base(name), nil
}

func (f *FS) Create(name string) (vfs.File, error) {
	if err := f.callHook("create", name); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	d, base, err := f.lookup("create", name)
	if err != nil {
		return nil, err
	}
	if _, exists := d.current[base]; exists {
		return nil, &fs.PathError{Op: "create", Path: name, Err: fs.ErrExist}
	}
	ino := &inode{}
	d.current[base] = ino
	d.pending = append(d.pending, dirOp{kind: "create", name: base, ino: ino})
	return &file{fs: f, ino: ino, gen: f.gen, name: name, writable: true}, nil
}

func (f *FS) Open(name string) (vfs.File, error) {
	return f.open("open", name, false)
}

func (f *FS) OpenAppend(name string) (vfs.File, error) {
	return f.open("openappend", name, true)
}

func (f *FS) open(op, name string, appendMode bool) (vfs.File, error) {
	if err := f.callHook(op, name); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	d, base, err := f.lookup(op, name)
	if err != nil {
		return nil, err
	}
	ino, ok := d.current[base]
	if !ok {
		return nil, &fs.PathError{Op: op, Path: name, Err: fs.ErrNotExist}
	}
	return &file{fs: f, ino: ino, gen: f.gen, name: name, writable: appendMode, append: appendMode}, nil
}

// Rename supports renames within one directory only, which is all the
// storage code does (temp name → final name).
func (f *FS) Rename(oldname, newname string) error {
	if err := f.callHook("rename", oldname); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	d, oldBase, err := f.lookup("rename", oldname)
	if err != nil {
		return err
	}
	if filepath.Dir(filepath.Clean(oldname)) != filepath.Dir(filepath.Clean(newname)) {
		return fmt.Errorf("crashfs: rename %s -> %s: cross-directory rename not supported", oldname, newname)
	}
	ino, ok := d.current[oldBase]
	if !ok {
		return &fs.PathError{Op: "rename", Path: oldname, Err: fs.ErrNotExist}
	}
	newBase := filepath.Base(newname)
	delete(d.current, oldBase)
	d.current[newBase] = ino
	d.pending = append(d.pending, dirOp{kind: "rename", name: newBase, from: oldBase})
	return nil
}

func (f *FS) Remove(name string) error {
	if err := f.callHook("remove", name); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	d, base, err := f.lookup("remove", name)
	if err != nil {
		return err
	}
	if _, ok := d.current[base]; !ok {
		return &fs.PathError{Op: "remove", Path: name, Err: fs.ErrNotExist}
	}
	delete(d.current, base)
	d.pending = append(d.pending, dirOp{kind: "remove", name: base})
	return nil
}

func (f *FS) List(dirPath string) ([]string, error) {
	if err := f.callHook("list", dirPath); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	d, ok := f.dirs[filepath.Clean(dirPath)]
	if !ok {
		return nil, &fs.PathError{Op: "list", Path: dirPath, Err: fs.ErrNotExist}
	}
	return sortedNames(d.current), nil
}

func (f *FS) SyncDir(dirPath string) error {
	if err := f.callHook("syncdir", dirPath); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	d, ok := f.dirs[filepath.Clean(dirPath)]
	if !ok {
		return &fs.PathError{Op: "syncdir", Path: dirPath, Err: fs.ErrNotExist}
	}
	d.durable = applyOps(d.durable, d.pending)
	d.pending = nil
	return nil
}

// Crash simulates power loss. Each directory keeps its durable entries plus
// the first policy.keepDirOps(n) of its n pending name changes. Each file still
// reachable keeps whatever policy.fileContents picks. Everything then becomes
// durable, as on a disk after reboot, and all existing handles go dead.
func (f *FS) Crash(policy Policy) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gen++
	survivors := map[*inode]*inode{} // old inode → its post-crash replacement

	// Iterate in sorted order. Go randomizes map iteration order, and with a
	// seeded policy a different order would give different results for the
	// same seed.
	for _, path := range sortedNames(f.dirs) {
		d := f.dirs[path]
		entries := applyOps(d.durable, d.pending[:policy.keepDirOps(len(d.pending))])
		next := map[string]*inode{}
		for _, name := range sortedNames(entries) {
			old := entries[name]
			if _, done := survivors[old]; !done {
				contents := bytes.Clone(policy.fileContents(old.durable, old.data))
				survivors[old] = &inode{data: contents, durable: bytes.Clone(contents)}
			}
			next[name] = survivors[old]
		}
		f.dirs[path] = &dir{current: next, durable: maps.Clone(next)}
	}
}

// applyOps returns a copy of entries with ops applied in order. entries is
// never nil (directories always start with an empty map), so neither is the
// copy that maps.Clone returns.
func applyOps(entries map[string]*inode, ops []dirOp) map[string]*inode {
	out := maps.Clone(entries)
	for _, op := range ops {
		switch op.kind {
		case "create":
			out[op.name] = op.ino
		case "remove":
			delete(out, op.name)
		case "rename":
			if ino, ok := out[op.from]; ok {
				delete(out, op.from)
				out[op.name] = ino
			}
		}
	}
	return out
}

func sortedNames[V any](m map[string]V) []string {
	names := make([]string, 0, len(m))
	for k := range m {
		names = append(names, k)
	}
	slices.Sort(names)
	return names
}

// Policy decides what survives a Crash. Use DropUnsynced or Random.
type Policy interface {
	keepDirOps(pending int) int
	fileContents(durable, current []byte) []byte
}

// DropUnsynced keeps only what was explicitly made durable: synced file data
// and synced directory entries. It is the harshest crash, and catches any
// missing Sync or SyncDir.
func DropUnsynced() Policy { return dropUnsynced{} }

type dropUnsynced struct{}

func (dropUnsynced) keepDirOps(int) int                    { return 0 }
func (dropUnsynced) fileContents(durable, _ []byte) []byte { return durable }

// Random explores the states a real disk could be left in:
//   - each directory keeps a random-length prefix of its pending name changes,
//     in order (as journaling filesystems such as ext4 replay them);
//   - an appended-to file keeps its durable bytes plus a random-length prefix
//     of the unsynced bytes (a torn write), and sometimes has whole 512-byte
//     sectors of that prefix zeroed (the file size reached disk before the data);
//   - a file changed in other ways (e.g. truncated) is either its durable or
//     its current version.
//
// The same rng seed always gives the same outcome.
func Random(rng *rand.Rand) Policy { return &random{rng: rng} }

type random struct{ rng *rand.Rand }

func (r *random) keepDirOps(pending int) int { return r.rng.IntN(pending + 1) }

const sectorSize = 512

func (r *random) fileContents(durable, current []byte) []byte {
	if len(current) < len(durable) || !bytes.HasPrefix(current, durable) {
		if r.rng.IntN(2) == 0 {
			return durable
		}
		return current
	}
	extra := len(current) - len(durable)
	var keep int
	switch r.rng.IntN(4) {
	case 0:
		keep = 0
	case 1:
		keep = extra
	default:
		keep = r.rng.IntN(extra + 1)
	}
	out := bytes.Clone(current[:len(durable)+keep])
	if keep > 0 && r.rng.IntN(4) == 0 {
		// Zero some sectors, but only bytes past the durable prefix: synced
		// data never changes.
		for start := (len(durable) / sectorSize) * sectorSize; start < len(out); start += sectorSize {
			if r.rng.IntN(2) == 0 {
				clear(out[max(start, len(durable)):min(start+sectorSize, len(out))])
			}
		}
	}
	return out
}

// file is an open handle. Its fields are guarded by fs.mu.
type file struct {
	fs       *FS
	ino      *inode
	gen      int
	name     string
	writable bool
	append   bool  // writes go to the end of the file (OpenAppend)
	off      int64 // read offset, and write offset when not in append mode
	closed   bool
}

// begin runs the hook, takes the lock and checks the handle is still usable.
// On success the caller must unlock f.fs.mu.
func (h *file) begin(op string) error {
	if err := h.fs.callHook(op, h.name); err != nil {
		return err
	}
	h.fs.mu.Lock()
	switch {
	case h.gen != h.fs.gen:
		h.fs.mu.Unlock()
		return ErrCrashed
	case h.closed:
		h.fs.mu.Unlock()
		return &fs.PathError{Op: op, Path: h.name, Err: os.ErrClosed}
	}
	return nil
}

func (h *file) Read(p []byte) (int, error) {
	if err := h.begin("read"); err != nil {
		return 0, err
	}
	defer h.fs.mu.Unlock()
	if h.off >= int64(len(h.ino.data)) {
		return 0, io.EOF
	}
	n := copy(p, h.ino.data[h.off:])
	h.off += int64(n)
	return n, nil
}

func (h *file) Write(p []byte) (int, error) {
	if err := h.begin("write"); err != nil {
		return 0, err
	}
	defer h.fs.mu.Unlock()
	if !h.writable {
		return 0, &fs.PathError{Op: "write", Path: h.name, Err: errors.New("file opened read-only")}
	}
	if h.append {
		h.off = int64(len(h.ino.data))
	}
	if end := int(h.off) + len(p); end > len(h.ino.data) {
		h.ino.data = append(h.ino.data, make([]byte, end-len(h.ino.data))...)
	}
	// In-place writes are safe: durable is always a separate copy (see Sync).
	copy(h.ino.data[h.off:], p)
	h.off += int64(len(p))
	return len(p), nil
}

func (h *file) Sync() error {
	if err := h.begin("sync"); err != nil {
		return err
	}
	defer h.fs.mu.Unlock()
	h.ino.durable = bytes.Clone(h.ino.data)
	return nil
}

func (h *file) Truncate(size int64) error {
	if err := h.begin("truncate"); err != nil {
		return err
	}
	defer h.fs.mu.Unlock()
	if !h.writable {
		return &fs.PathError{Op: "truncate", Path: h.name, Err: errors.New("file opened read-only")}
	}
	if size < 0 {
		return &fs.PathError{Op: "truncate", Path: h.name, Err: fs.ErrInvalid}
	}
	if size < int64(len(h.ino.data)) {
		h.ino.data = h.ino.data[:size]
	} else {
		h.ino.data = append(h.ino.data, make([]byte, size-int64(len(h.ino.data)))...)
	}
	return nil
}

func (h *file) Close() error {
	if err := h.begin("close"); err != nil {
		return err
	}
	defer h.fs.mu.Unlock()
	h.closed = true
	return nil
}
