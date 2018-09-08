package phantomObjects

import (
	"s3-sftp-proxy/s3path"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPhantomObjectMapAdd(t *testing.T) {
	pom := NewPhantomObjectMap()
	assert.Equal(t, true, pom.Add(&PhantomObjectInfo{Key: s3path.Path{"", "a", "b"}}))
	assert.Equal(t, 1, pom.Size())
	assert.Equal(t, false, pom.Add(&PhantomObjectInfo{Key: s3path.Path{"", "a", "b"}}))
	assert.Equal(t, 1, pom.Size())
	assert.Equal(t, true, pom.Add(&PhantomObjectInfo{Key: s3path.Path{"", "a", "c"}}))
	assert.Equal(t, 2, pom.Size())
	assert.Equal(t, true, pom.Add(&PhantomObjectInfo{Key: s3path.Path{"", "a", "b", "c"}}))
	assert.Equal(t, 3, pom.Size())
}

func TestPhantomObjectMapRemove(t *testing.T) {
	pom := NewPhantomObjectMap()
	o1 := &PhantomObjectInfo{Key: s3path.Path{"", "a", "b"}}
	o2 := &PhantomObjectInfo{Key: s3path.Path{"", "a", "b"}}
	o3 := &PhantomObjectInfo{Key: s3path.Path{"", "a", "c"}}
	o4 := &PhantomObjectInfo{Key: s3path.Path{"", "a", "b", "c"}}
	assert.Equal(t, true, pom.Add(o1))
	assert.Equal(t, 1, pom.Size())
	assert.Equal(t, false, pom.Add(o2))
	assert.Equal(t, 1, pom.Size())
	assert.Equal(t, true, pom.Add(o3))
	assert.Equal(t, 2, pom.Size())
	assert.Equal(t, true, pom.Add(o4))
	assert.Equal(t, 3, pom.Size())
	assert.Equal(t, o3, pom.Remove(s3path.Path{"", "a", "c"}))
	assert.Nil(t, pom.Get(s3path.Path{"", "a", "c"}))
	assert.Equal(t, 2, pom.Size())
	assert.Nil(t, pom.Remove(s3path.Path{"", "a", "c"}))
	assert.Equal(t, 2, pom.Size())
	assert.Equal(t, o2, pom.Remove(s3path.Path{"", "a", "b"}))
	assert.Equal(t, 1, pom.Size())
	assert.Nil(t, pom.Get(s3path.Path{"", "a", "b"}))

}
