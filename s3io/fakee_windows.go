// +build windows

package s3io

import "syscall"

func buildFakeFileInfoSys() interface{} {
	return syscall.Win32FileAttributeData{}
}
