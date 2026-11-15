// An external test package (vfs_test, not vfs) so it can import vfstest,
// which itself imports vfs. Inside package vfs that would be an import cycle.
package vfs_test

import (
	"testing"

	"github.com/dev64-kartik/kvstore/internal/vfs"
	"github.com/dev64-kartik/kvstore/internal/vfs/vfstest"
)

func TestOSConformance(t *testing.T) {
	vfstest.Run(t, func(t *testing.T) (vfs.FS, string) {
		return vfs.OS(), t.TempDir()
	})
}
