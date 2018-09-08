package s3io

import (
	"context"
	"fmt"
	"os"
	"s3-sftp-proxy/byteswriter"
	"s3-sftp-proxy/logging"
	"s3-sftp-proxy/phantomObjects"

	"s3-sftp-proxy/s3path"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
)

type S3ObjectStat struct {
	logging.DebugLogger
	Ctx              context.Context
	Bucket           string
	Key              s3path.Path
	S3               *aws_s3.S3
	PhantomObjectMap *phantomObjects.PhantomObjectMap
}

func (sos *S3ObjectStat) ListAt(result []os.FileInfo, o int64) (int, error) {
	F(sos.Debug, "S3ObjectStat.ListAt: len(result)=%d offset=%d", len(result), o)
	_o, err := byteswriter.CastInt64ToInt(o)
	if err != nil {
		return 0, err
	}

	if len(result) == 0 {
		return 0, nil
	}

	if _o > 0 {
		return 0, fmt.Errorf("supplied position is out of range")
	}

	if sos.Key.IsRoot() {
		result[0] = NewObjectFileInfo(
			"/",
			time.Time{},
			0,
			0755|os.ModeDir,
		)
	} else {
		phInfo := sos.PhantomObjectMap.Get(sos.Key)
		if phInfo != nil {
			_phInfo := phInfo.GetOne()
			result[0] = NewObjectFileInfo(
				_phInfo.Key.Base(),
				_phInfo.LastModified,
				_phInfo.Size,
				0600, // TODO
			)
		} else {
			key := sos.Key.String()
			F(sos.Debug, "GetObjectAclWithContext(Bucket=%s, Key=%s)", sos.Bucket, key)
			out, err := sos.S3.GetObjectAclWithContext(
				sos.Ctx,
				&aws_s3.GetObjectAclInput{
					Bucket: &sos.Bucket,
					Key:    &key,
				},
			)
			if err == nil {
				F(sos.Debug, "=> %v", out)
				F(sos.Debug, "HeadObjectWithContext(Bucket=%s, Key=%s)", sos.Bucket, key)
				headOut, err := sos.S3.HeadObjectWithContext(
					sos.Ctx,
					&aws_s3.HeadObjectInput{
						Bucket: &sos.Bucket,
						Key:    &key,
					},
				)

				// objInfo := ObjectFileInfo{
				// 	_Name: sos.Key.Base(),
				// 	_Mode: aclToMode(out.Owner, out.Grants),
				// }
				// TODO why only to vals supplied

				var objInfo *ObjectFileInfo
				if err != nil {
					F(sos.Debug, "=> { ContentLength=%d, LastModified=%v, Error=%+v}", *headOut.ContentLength, *headOut.LastModified, err)
					objInfo = NewObjectFileInfo(sos.Key.Base(), time.Now(), 0, aclToMode(out.Owner, out.Grants))
				} else {
					F(sos.Debug, "=> { ContentLength=%d, LastModified=%v }", *headOut.ContentLength, *headOut.LastModified)
					objInfo = NewObjectFileInfo(sos.Key.Base(), *headOut.LastModified, *headOut.ContentLength, aclToMode(out.Owner, out.Grants))
				}

				result[0] = objInfo
			} else {
				sos.Debug("=> ", err)
				F(sos.Debug, "ListObjectsV2WithContext(Bucket=%s, Prefix=%s)", sos.Bucket, key)
				out, err := sos.S3.ListObjectsV2WithContext(
					sos.Ctx,
					&aws_s3.ListObjectsV2Input{
						Bucket:    &sos.Bucket,
						Prefix:    &key,
						MaxKeys:   aws.Int64(10000),
						Delimiter: aws.String("/"),
					},
				)
				if err != nil || len(out.CommonPrefixes) == 0 {
					sos.Debug("=> ", err)
					return 0, os.ErrNotExist
				}
				F(sos.Debug, "=> { CommonPrefixes=len(%d), Contents=len(%d) }", len(out.CommonPrefixes), len(out.Contents))

				result[0] = NewObjectFileInfo(sos.Key.Base(), time.Now(), 0, 0755|os.ModeDir)

			}
		}
	}
	return 1, nil
}
