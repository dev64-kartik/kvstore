package crashfs

import (
	"errors"
	"io/fs"
	"math/rand/v2"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/dev64-kartik/kvstore/internal/vfs"
	"github.com/dev64-kartik/kvstore/internal/vfs/vfstest"
)

const testDir = "/data"

func newFS() *FS {
	f := New()
	f.Mkdir(testDir)
	return f
}

func path(name string) string { return filepath.Join(testDir, name) }

// Without a crash, crashfs must behave exactly like the real disk.
func TestConformance(t *testing.T) {
	vfstest.Run(t, func(t *testing.T) (vfs.FS, string) { return newFS(), testDir })
}

// write creates name with contents, syncs the file and (if syncDir) the directory.
func write(t *testing.T, f *FS, name, contents string, syncDir bool) {
	t.Helper()
	vfstest.WriteFile(t, f, path(name), contents)
	if syncDir {
		if err := f.SyncDir(testDir); err != nil {
			t.Fatal(err)
		}
	}
}

func mustAppend(t *testing.T, f *FS, name, contents string) vfs.File {
	t.Helper()
	h, err := f.OpenAppend(path(name))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.Write([]byte(contents)); err != nil {
		t.Fatal(err)
	}
	return h
}

func TestDropUnsyncedLosesUnsyncedWrite(t *testing.T) {
	f := newFS()
	write(t, f, "wal", "synced|", true)
	mustAppend(t, f, "wal", "unsynced") // never synced
	f.Crash(DropUnsynced())
	if got := vfstest.ReadFile(t, f, path("wal")); got != "synced|" {
		t.Fatalf("after crash: %q, want \"synced|\"", got)
	}
}

// The WAL's torn-tail repair path: truncate, then append over the cut. Until a
// Sync, a crash must bring back the old synced bytes intact. This is the only
// test that writes *inside* an already-synced range, so it is what catches
// Sync sharing storage with live data instead of copying it.
func TestDropUnsyncedRevertsTruncateAndOverwrite(t *testing.T) {
	f := newFS()
	write(t, f, "wal", "abcdef", true)
	h, err := f.OpenAppend(path("wal"))
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Truncate(3); err != nil {
		t.Fatal(err)
	}
	if _, err := h.Write([]byte("X")); err != nil {
		t.Fatal(err)
	}
	f.Crash(DropUnsynced())
	if got := vfstest.ReadFile(t, f, path("wal")); got != "abcdef" {
		t.Fatalf("after crash: %q, want \"abcdef\"", got)
	}
}

// Edge case 1.9: the file's data was synced, but its name never was.
func TestSyncedFileWithoutDirSyncVanishes(t *testing.T) {
	f := newFS()
	write(t, f, "seg", "data", false)
	f.Crash(DropUnsynced())
	if names := vfstest.List(t, f, testDir); len(names) != 0 {
		t.Fatalf("after crash: List = %q, want empty", names)
	}
}

func TestRenameAndRemoveNeedDirSync(t *testing.T) {
	f := newFS()
	write(t, f, "a.tmp", "A", true)
	write(t, f, "b", "B", true)
	if err := f.Rename(path("a.tmp"), path("a")); err != nil {
		t.Fatal(err)
	}
	if err := f.Remove(path("b")); err != nil {
		t.Fatal(err)
	}
	f.Crash(DropUnsynced())
	if names := vfstest.List(t, f, testDir); !slices.Equal(names, []string{"a.tmp", "b"}) {
		t.Fatalf("unsynced rename+remove: List = %q, want [a.tmp b]", names)
	}

	if err := f.Rename(path("a.tmp"), path("a")); err != nil {
		t.Fatal(err)
	}
	if err := f.SyncDir(testDir); err != nil {
		t.Fatal(err)
	}
	f.Crash(DropUnsynced())
	if names := vfstest.List(t, f, testDir); !slices.Equal(names, []string{"a", "b"}) {
		t.Fatalf("synced rename: List = %q, want [a b]", names)
	}
}

// Random keeps the durable bytes plus some prefix of the unsynced ones, each
// unsynced byte either real or zeroed. Across seeds it must produce every
// outcome: nothing kept, a torn prefix, everything kept, and zeroed sectors.
func TestRandomTornWrites(t *testing.T) {
	const durable = "D"
	unsynced := strings.Repeat("u", 3*sectorSize)
	var sawNone, sawPartial, sawAll, sawZeros bool
	for seed := range uint64(200) {
		f := newFS()
		write(t, f, "wal", durable, true)
		mustAppend(t, f, "wal", unsynced)
		f.Crash(Random(rand.New(rand.NewPCG(seed, 0))))

		got := vfstest.ReadFile(t, f, path("wal"))
		if !strings.HasPrefix(got, durable) || len(got) > len(durable)+len(unsynced) {
			t.Fatalf("seed %d: %d bytes, not durable prefix + part of unsynced", seed, len(got))
		}
		tail := got[len(durable):]
		if strings.Trim(tail, "u\x00") != "" {
			t.Fatalf("seed %d: unsynced region holds bytes that were never written", seed)
		}
		switch len(tail) {
		case 0:
			sawNone = true
		case len(unsynced):
			sawAll = true
		default:
			sawPartial = true
		}
		sawZeros = sawZeros || strings.Contains(tail, "\x00")
	}
	if !sawNone || !sawPartial || !sawAll || !sawZeros {
		t.Fatalf("outcomes not all explored: none=%v partial=%v all=%v zeros=%v", sawNone, sawPartial, sawAll, sawZeros)
	}
}

// Unsynced name changes survive as an ordered prefix: create a, create b,
// remove a can only leave {}, {a}, {a b} or {b}, and Random must reach all four.
func TestRandomDirOpsSurviveAsPrefix(t *testing.T) {
	allowed := map[string]bool{"": false, "a": false, "a,b": false, "b": false}
	for seed := range uint64(100) {
		f := newFS()
		write(t, f, "a", "A", false)
		write(t, f, "b", "B", false)
		if err := f.Remove(path("a")); err != nil {
			t.Fatal(err)
		}
		f.Crash(Random(rand.New(rand.NewPCG(seed, 0))))
		state := strings.Join(vfstest.List(t, f, testDir), ",")
		if _, ok := allowed[state]; !ok {
			t.Fatalf("seed %d: impossible directory state [%s]", seed, state)
		}
		allowed[state] = true
	}
	for state, seen := range allowed {
		if !seen {
			t.Errorf("state [%s] never produced", state)
		}
	}
}

func TestCrashIsDeterministic(t *testing.T) {
	run := func() string {
		f := newFS()
		for _, name := range []string{"x", "y", "z"} {
			write(t, f, name, "durable-"+name, true)
			mustAppend(t, f, name, strings.Repeat(name, 2000))
		}
		f.Crash(Random(rand.New(rand.NewPCG(42, 0))))
		var out []string
		for _, name := range vfstest.List(t, f, testDir) {
			out = append(out, name+"="+vfstest.ReadFile(t, f, path(name)))
		}
		return strings.Join(out, ";")
	}
	if first, second := run(), run(); first != second {
		t.Fatal("same seed produced different post-crash states")
	}
}

func TestHandlesDieOnCrash(t *testing.T) {
	f := newFS()
	write(t, f, "wal", "x", true)
	h := mustAppend(t, f, "wal", "y")
	f.Crash(DropUnsynced())
	if _, err := h.Write([]byte("z")); !errors.Is(err, ErrCrashed) {
		t.Fatalf("Write on old handle: err = %v, want ErrCrashed", err)
	}
	if err := h.Sync(); !errors.Is(err, ErrCrashed) {
		t.Fatalf("Sync on old handle: err = %v, want ErrCrashed", err)
	}
	// The FS itself (the "disk") is usable after the crash.
	mustAppend(t, f, "wal", "after")
}

// A hook failing Sync leaves the data non-durable.
func TestHookFailsSync(t *testing.T) {
	f := newFS()
	write(t, f, "wal", "ok|", true)
	errDisk := errors.New("injected EIO")
	f.SetHook(func(op, _ string) error {
		if op == "sync" {
			return errDisk
		}
		return nil
	})
	h := mustAppend(t, f, "wal", "lost")
	if err := h.Sync(); !errors.Is(err, errDisk) {
		t.Fatalf("Sync: err = %v, want injected error", err)
	}
	f.SetHook(nil)
	f.Crash(DropUnsynced())
	if got := vfstest.ReadFile(t, f, path("wal")); got != "ok|" {
		t.Fatalf("after failed sync + crash: %q, want \"ok|\"", got)
	}
}

// The hook can crash the FS at an exact operation, which the step 8 crash
// harness relies on. The interrupted operation itself never happens.
func TestHookCanCrashMidOperation(t *testing.T) {
	f := newFS()
	write(t, f, "wal", "1", true)
	h := mustAppend(t, f, "wal", "2")
	if err := h.Sync(); err != nil {
		t.Fatal(err)
	}
	f.SetHook(func(op, _ string) error {
		if op == "write" {
			f.Crash(DropUnsynced())
			return ErrCrashed
		}
		return nil
	})
	if _, err := h.Write([]byte("3")); !errors.Is(err, ErrCrashed) {
		t.Fatalf("Write: err = %v, want ErrCrashed", err)
	}
	f.SetHook(nil)
	if got := vfstest.ReadFile(t, f, path("wal")); got != "12" {
		t.Fatalf("after crash: %q, want \"12\"", got)
	}
}

func TestMissingDirectory(t *testing.T) {
	f := New()
	if _, err := f.Create("/nope/f"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("Create in missing dir: err = %v, want fs.ErrNotExist", err)
	}
}
