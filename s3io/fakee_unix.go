// +build !windows

package s3io

import (
	"syscall"
)

func buildFakeFileInfoSys() interface{} {
	return &syscall.Stat_t{Uid: 65534, Gid: 65534}
}
