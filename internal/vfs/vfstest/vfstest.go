// Package vfstest is a conformance suite for vfs.FS implementations. Every FS
// (the real disk, and the crash-simulating one) must pass it, so storage code
// tested against the fake behaves the same on a real disk.
package vfstest

import (
	"errors"
	"io"
	"io/fs"
	"path/filepath"
	"slices"
	"testing"

	"github.com/dev64-kartik/kvstore/internal/vfs"
)

// NewFS returns a fresh FS and the path of an existing, empty directory in it.
type NewFS func(t *testing.T) (vfs.FS, string)

// Run checks the behaviors the WAL and snapshot code rely on. It covers normal
// operation only; crash behavior is tested by each implementation itself.
func Run(t *testing.T, newFS NewFS) {
	t.Run("CreateWriteReadBack", func(t *testing.T) {
		fsys, dir := newFS(t)
		p := filepath.Join(dir, "f")
		WriteFile(t, fsys, p, "hello")
		if got := ReadFile(t, fsys, p); got != "hello" {
			t.Fatalf("read %q, want %q", got, "hello")
		}
	})

	t.Run("CreateExistingFails", func(t *testing.T) {
		fsys, dir := newFS(t)
		p := filepath.Join(dir, "f")
		WriteFile(t, fsys, p, "x")
		if _, err := fsys.Create(p); !errors.Is(err, fs.ErrExist) {
			t.Fatalf("Create on existing file: err = %v, want fs.ErrExist", err)
		}
	})

	t.Run("MissingFileIsNotExist", func(t *testing.T) {
		fsys, dir := newFS(t)
		p := filepath.Join(dir, "missing")
		f, err := fsys.Open(p)
		if !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("Open: err = %v, want fs.ErrNotExist", err)
		}
		// On error the File must be a true nil interface, not an interface
		// wrapping a nil pointer, or callers' `f != nil` checks lie.
		if f != nil {
			t.Fatalf("Open returned non-nil File %#v alongside an error", f)
		}
		if _, err := fsys.OpenAppend(p); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("OpenAppend: err = %v, want fs.ErrNotExist", err)
		}
		if err := fsys.Remove(p); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("Remove: err = %v, want fs.ErrNotExist", err)
		}
	})

	t.Run("OpenIsReadOnly", func(t *testing.T) {
		fsys, dir := newFS(t)
		p := filepath.Join(dir, "f")
		WriteFile(t, fsys, p, "x")
		f, err := fsys.Open(p)
		if err != nil {
			t.Fatal(err)
		}
		defer closeOrFail(t, f)
		if _, err := f.Write([]byte("y")); err == nil {
			t.Fatal("Write on a file opened with Open succeeded")
		}
	})

	// The WAL's recovery path: read the last segment, cut off a torn tail,
	// then keep appending at the cut.
	t.Run("OpenAppendReadTruncateAppend", func(t *testing.T) {
		fsys, dir := newFS(t)
		p := filepath.Join(dir, "f")
		WriteFile(t, fsys, p, "abcdef")
		f, err := fsys.OpenAppend(p)
		if err != nil {
			t.Fatal(err)
		}
		got, err := io.ReadAll(f)
		if err != nil || string(got) != "abcdef" {
			t.Fatalf("ReadAll = %q, %v; want \"abcdef\"", got, err)
		}
		if err := f.Truncate(3); err != nil {
			t.Fatal(err)
		}
		if _, err := f.Write([]byte("X")); err != nil {
			t.Fatal(err)
		}
		syncAndClose(t, f)
		if got := ReadFile(t, fsys, p); got != "abcX" {
			t.Fatalf("after truncate+append: %q, want \"abcX\"", got)
		}
	})

	// The WAL writer creates a segment under a temp name, renames it, then
	// keeps writing through the same handle.
	t.Run("RenameKeepsOpenHandle", func(t *testing.T) {
		fsys, dir := newFS(t)
		tmp, final := filepath.Join(dir, "seg.tmp"), filepath.Join(dir, "seg")
		f, err := fsys.Create(tmp)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.Write([]byte("head")); err != nil {
			t.Fatal(err)
		}
		if err := fsys.Rename(tmp, final); err != nil {
			t.Fatal(err)
		}
		if _, err := f.Write([]byte("+tail")); err != nil {
			t.Fatal(err)
		}
		syncAndClose(t, f)
		if got := ReadFile(t, fsys, final); got != "head+tail" {
			t.Fatalf("read %q, want \"head+tail\"", got)
		}
		if names := List(t, fsys, dir); !slices.Equal(names, []string{"seg"}) {
			t.Fatalf("List = %q, want [seg]", names)
		}
	})

	t.Run("RemoveAndListSorted", func(t *testing.T) {
		fsys, dir := newFS(t)
		for _, name := range []string{"c", "a", "d", "b"} {
			WriteFile(t, fsys, filepath.Join(dir, name), name)
		}
		if err := fsys.Remove(filepath.Join(dir, "d")); err != nil {
			t.Fatal(err)
		}
		if names := List(t, fsys, dir); !slices.Equal(names, []string{"a", "b", "c"}) {
			t.Fatalf("List = %q, want [a b c]", names)
		}
	})

	t.Run("SyncDir", func(t *testing.T) {
		fsys, dir := newFS(t)
		WriteFile(t, fsys, filepath.Join(dir, "f"), "x")
		if err := fsys.SyncDir(dir); err != nil {
			t.Fatalf("SyncDir: %v", err)
		}
	})
}

// WriteFile creates path with the given contents and syncs it. It does not
// sync the directory.
func WriteFile(t *testing.T, fsys vfs.FS, path, contents string) {
	t.Helper()
	f, err := fsys.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte(contents)); err != nil {
		t.Fatal(err)
	}
	syncAndClose(t, f)
}

// ReadFile returns the full contents of path.
func ReadFile(t *testing.T, fsys vfs.FS, path string) string {
	t.Helper()
	f, err := fsys.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer closeOrFail(t, f)
	b, err := io.ReadAll(f)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// List returns List(dir) or fails the test.
func List(t *testing.T, fsys vfs.FS, dir string) []string {
	t.Helper()
	names, err := fsys.List(dir)
	if err != nil {
		t.Fatal(err)
	}
	return names
}

func syncAndClose(t *testing.T, f vfs.File) {
	t.Helper()
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	closeOrFail(t, f)
}

func closeOrFail(t *testing.T, f vfs.File) {
	t.Helper()
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}
