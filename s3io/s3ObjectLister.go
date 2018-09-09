package s3io

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"s3-sftp-proxy/byteswriter"
	"s3-sftp-proxy/logging"
	"s3-sftp-proxy/phantomObjects"

	"s3-sftp-proxy/s3path"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

type S3ObjectLister struct {
	logging.DebugLogger
	Ctx              context.Context
	Bucket           string
	Prefix           s3path.Path
	S3               s3.S3
	Lookback         int
	PhantomObjectMap *phantomObjects.PhantomObjectMap
	spoolOffset      int
	spooled          []os.FileInfo
	continuation     *string
	noMore           bool
}

func (sol *S3ObjectLister) ListAt(result []os.FileInfo, o int64) (int, error) {
	_o, err := byteswriter.CastInt64ToInt(o)
	if err != nil {
		return 0, err
	}

	if _o < sol.spoolOffset {
		return 0, fmt.Errorf("supplied position is out of range")
	}

	s := _o - sol.spoolOffset
	i := 0
	if s < len(sol.spooled) {
		n := len(result)
		if n > len(sol.spooled)-s {
			n = len(sol.spooled) - s
		}
		copy(result[i:i+n], sol.spooled[s:s+n])
		i += n
		s = len(sol.spooled)
	}

	if i >= len(result) {
		return i, nil
	}

	if sol.noMore {
		if i == 0 {
			return 0, io.EOF
		} else {
			return i, nil
		}
	}

	if s <= len(sol.spooled) && s >= sol.Lookback {
		sol.spooled = sol.spooled[s-sol.Lookback:]
		sol.spoolOffset += s - sol.Lookback
		s = sol.Lookback
	}

	if sol.continuation == nil {
		sol.spooled = append(sol.spooled, NewObjectFileInfo(".", time.Unix(1, 0), 0, 0755|os.ModeDir))
		sol.spooled = append(sol.spooled, NewObjectFileInfo("..", time.Unix(1, 0), 0, 0755|os.ModeDir))

		phObjs := sol.PhantomObjectMap.List(sol.Prefix)
		for _, phInfo := range phObjs {
			_phInfo := phInfo.GetOne()
			sol.spooled = append(sol.spooled, NewObjectFileInfo(_phInfo.Key.Base(), _phInfo.LastModified, _phInfo.Size, 0600 /* TODO*/))
		}
	}

	prefix := sol.Prefix.String()
	if prefix != "" {
		prefix += "/"
	}
	F(sol.Debug, "ListObjectsV2WithContext(Bucket=%s, Prefix=%s, Continuation=%v)", sol.Bucket, prefix, sol.continuation)
	out, err := sol.S3.ListObjectsV2WithContext(
		sol.Ctx,
		&s3.ListObjectsV2Input{
			Bucket:            &sol.Bucket,
			Prefix:            &prefix,
			MaxKeys:           aws.Int64(10000),
			Delimiter:         aws.String("/"),
			ContinuationToken: sol.continuation,
		},
	)
	if err != nil {
		sol.Debug("=> ", err)
		return i, err
	}
	F(sol.Debug, "=> { CommonPrefixes=len(%d), Contents=len(%d) }", len(out.CommonPrefixes), len(out.Contents))

	if sol.continuation == nil {
		for _, cPfx := range out.CommonPrefixes {

			sol.spooled = append(sol.spooled, NewObjectFileInfo(path.Base(*cPfx.Prefix), time.Unix(1, 0), 0, 0755|os.ModeDir))
		}
	}
	for _, obj := range out.Contents {
		// if *obj.Key == sol.Prefix {
		// 	continue
		// }

		sol.spooled = append(sol.spooled, NewObjectFileInfo(
			path.Base(*obj.Key),
			*obj.LastModified,
			*obj.Size,
			0644,
		))
	}
	sol.continuation = out.NextContinuationToken
	if out.NextContinuationToken == nil {
		sol.noMore = true
	}

	var n int
	if len(sol.spooled)-s > len(result)-i {
		n = len(result) - i
	} else {
		n = len(sol.spooled) - s
		if sol.noMore {
			err = io.EOF
		}
	}

	copy(result[i:i+n], sol.spooled[s:s+n])
	return i + n, err
}
