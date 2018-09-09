package s3io

import (
	"context"
	"fmt"
	"io"
	"s3-sftp-proxy/byteswriter"
	"s3-sftp-proxy/logging"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/service/s3"
)

type S3GetObjectOutputReader struct {
	Ctx          context.Context
	Goo          *s3.GetObjectOutput
	Log          logging.DebugLogger
	Lookback     int
	MinChunkSize int
	mtx          sync.Mutex
	spooled      []byte
	spoolOffset  int
	noMore       bool
}

func (oor *S3GetObjectOutputReader) Close() error {
	if oor.Goo.Body != nil {
		oor.Goo.Body.Close()
		oor.Goo.Body = nil
	}
	return nil
}

type ReadDeadlineSettable interface {
	SetReadDeadline(t time.Time) error
}

func (oor *S3GetObjectOutputReader) ReadAt(buf []byte, off int64) (int, error) {
	oor.mtx.Lock()
	defer oor.mtx.Unlock()

	F(oor.Log.Debug, "len(buf)=%d, off=%d", len(buf), off)
	_o, err := byteswriter.CastInt64ToInt(off)
	if err != nil {
		return 0, err
	}
	if _o < oor.spoolOffset {
		return 0, fmt.Errorf("supplied position is out of range")
	}

	s := _o - oor.spoolOffset
	i := 0
	r := len(buf)
	if s < len(oor.spooled) {
		// n = max(r, len(oor.spooled)-s)
		n := r
		if n > len(oor.spooled)-s {
			n = len(oor.spooled) - s
		}
		copy(buf[i:i+n], oor.spooled[s:s+n])
		i += n
		s += n
		r -= n
	}
	if r == 0 {
		return i, nil
	}

	if oor.noMore {
		if i == 0 {
			return 0, io.EOF
		} else {
			return i, nil
		}
	}

	F(oor.Log.Debug, "s=%d, len(oor.spooled)=%d, oor.Lookback=%d", s, len(oor.spooled), oor.Lookback)
	if s <= len(oor.spooled) && s >= oor.Lookback {
		oor.spooled = oor.spooled[s-oor.Lookback:]
		oor.spoolOffset += s - oor.Lookback
		s = oor.Lookback
	}

	var e int
	if len(oor.spooled)+oor.MinChunkSize < s+r {
		e = s + r
	} else {
		e = len(oor.spooled) + oor.MinChunkSize
	}

	if cap(oor.spooled) < e {
		spooled := make([]byte, len(oor.spooled), e)
		copy(spooled, oor.spooled)
		oor.spooled = spooled
	}

	type readResult struct {
		n   int
		err error
	}

	resultChan := make(chan readResult)
	go func() {
		n, err := io.ReadFull(oor.Goo.Body, oor.spooled[len(oor.spooled):e])
		resultChan <- readResult{n, err}
	}()
	select {
	case <-oor.Ctx.Done():
		oor.Goo.Body.(ReadDeadlineSettable).SetReadDeadline(time.Unix(1, 0))
		oor.Log.Debug("canceled")
		return 0, fmt.Errorf("read operation canceled")
	case res := <-resultChan:
		if IsEOF(res.err) {
			oor.noMore = true
		}
		e = len(oor.spooled) + res.n
		oor.spooled = oor.spooled[:e]
		if s < e {
			be := e
			if be > s+r {
				be = s + r
			}
			copy(buf[i:], oor.spooled[s:be])
			return be - s, nil
		} else {
			return 0, io.EOF
		}
	}
}
