// An external test package (vfs_test, not vfs) so it can import vfstest,
// which itself imports vfs. Inside package vfs that would be an import cycle.
package vfs_test

import (
	"errors"
	"io/fs"
	"path/filepath"
	"testing"

	"github.com/dev64-kartik/kvstore/internal/vfs"
	"github.com/dev64-kartik/kvstore/internal/vfs/vfstest"
)

func TestReadFile(t *testing.T) {
	fsys, dir := vfs.OS(), t.TempDir()
	p := filepath.Join(dir, "f")
	vfstest.WriteFile(t, fsys, p, "contents")
	if got, err := vfs.ReadFile(fsys, p); err != nil || string(got) != "contents" {
		t.Fatalf("ReadFile = %q, %v", got, err)
	}
	if _, err := vfs.ReadFile(fsys, filepath.Join(dir, "missing")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("missing file: err = %v, want fs.ErrNotExist", err)
	}
}

func TestOSConformance(t *testing.T) {
	vfstest.Run(t, func(t *testing.T) (vfs.FS, string) {
		return vfs.OS(), t.TempDir()
	})
}
