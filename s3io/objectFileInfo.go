package s3io

import (
	"os"
	"time"
)

func NewObjectFileInfo(name string, lastMod time.Time, size int64, mode os.FileMode) *ObjectFileInfo {
	return &ObjectFileInfo{
		name:         name,
		lastModified: lastMod,
		size:         size,
		mode:         mode,
	}
}

type ObjectFileInfo struct {
	name         string
	lastModified time.Time
	size         int64
	mode         os.FileMode
}

func (ofi *ObjectFileInfo) Name() string {
	return ofi.name
}

func (ofi *ObjectFileInfo) ModTime() time.Time {
	return ofi.lastModified
}

func (ofi *ObjectFileInfo) Size() int64 {
	return ofi.size
}

func (ofi *ObjectFileInfo) Mode() os.FileMode {
	return ofi.mode
}

func (ofi *ObjectFileInfo) IsDir() bool {
	return (ofi.mode & os.ModeDir) != 0
}

func (ofi *ObjectFileInfo) Sys() interface{} {
	return buildFakeFileInfoSys()
}
