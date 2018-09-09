package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"s3-sftp-proxy/byteswriter"
	"s3-sftp-proxy/s3path"

	"s3-sftp-proxy/config"
	"s3-sftp-proxy/logging"

	"s3-sftp-proxy/phantomObjects"
	"s3-sftp-proxy/s3io"
	"time"

	aws "github.com/aws/aws-sdk-go/aws"
	aws_session "github.com/aws/aws-sdk-go/aws/session"
	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/pkg/sftp"
	// s3crypto "github.com/aws/aws-sdk-go/service/s3/s3crypto"
)

var aclPrivate = "private"
var sseTypes = map[config.ServerSideEncryptionType]*string{
	config.ServerSideEncryptionTypeKMS: aws.String("aws:kms"),
}

type WriteDeadlineSettable interface {
	SetWriteDeadline(t time.Time) error
}

func nilIfEmpty(s string) *string {
	if s == "" {
		return nil
	} else {
		return &s
	}
}

func aclToMode(owner *aws_s3.Owner, grants []*aws_s3.Grant) os.FileMode {
	var v os.FileMode
	for _, g := range grants {
		if g.Grantee != nil {
			if g.Grantee.ID != nil && *g.Grantee.ID == *owner.ID {
				switch *g.Permission {
				case "READ":
					v |= 0400
				case "WRITE":
					v |= 0200
				case "FULL_CONTROL":
					v |= 0600
				}
			} else if g.Grantee.URI != nil {
				switch *g.Grantee.URI {
				case "http://acs.amazonaws.com/groups/global/AuthenticatedUsers":
					switch *g.Permission {
					case "READ":
						v |= 0440
					case "WRITE":
						v |= 0220
					case "FULL_CONTROL":
						v |= 0660
					}
				case "http://acs.amazonaws.com/groups/global/AllUsers":
					switch *g.Permission {
					case "READ":
						v |= 0444
					case "WRITE":
						v |= 0222
					case "FULL_CONTROL":
						v |= 0666
					}
				}
			}
		}
	}
	return v
}

type S3BucketIO struct {
	Ctx                      context.Context
	Bucket                   *S3Bucket
	ReaderLookbackBufferSize int
	ReaderMinChunkSize       int
	ListerLookbackBufferSize int
	PhantomObjectMap         *phantomObjects.PhantomObjectMap
	Perms                    Perms
	ServerSideEncryption     *config.ServerSideEncryptionConfig
	Now                      func() time.Time
	Log                      interface {
		logging.ErrorLogger
		logging.DebugLogger
	}
}

func buildKey(s3b *S3Bucket, p string) s3path.Path {
	return s3b.KeyPrefix.Join(s3path.SplitIntoPath(p))
}

func buildPath(s3b *S3Bucket, key string) (string, bool) {
	_key := s3path.SplitIntoPath(key)
	if !_key.IsPrefixed(s3b.KeyPrefix) {
		return "", false
	}
	return "/" + _key[len(s3b.KeyPrefix):].String(), true
}

func (s3Bio *S3BucketIO) Fileread(req *sftp.Request) (io.ReaderAt, error) {
	if !s3Bio.Perms.Readable {
		return nil, fmt.Errorf("read operation not allowed as per configuration")
	}
	sess, err := aws_session.NewSession()
	if err != nil {
		return nil, err
	}
	s3 := s3Bio.Bucket.S3(sess)
	key := buildKey(s3Bio.Bucket, req.Filepath)

	phInfo := s3Bio.PhantomObjectMap.Get(key)
	if phInfo != nil {
		return bytes.NewReader(phInfo.Opaque.(*s3io.S3PutObjectWriter).Writer.Bytes()), nil
	}

	keyStr := key.String()
	ctx := combineContext(s3Bio.Ctx, req.Context())
	s3io.F(s3Bio.Log.Debug, "GetObject(Bucket=%s, Key=%s)", s3Bio.Bucket.Bucket, keyStr)
	sse := s3Bio.ServerSideEncryption
	goo, err := s3.GetObjectWithContext(
		ctx,
		&aws_s3.GetObjectInput{
			Bucket:               &s3Bio.Bucket.Bucket,
			Key:                  &keyStr,
			SSECustomerAlgorithm: nilIfEmpty(sse.CustomerAlgorithm()),
			SSECustomerKey:       nilIfEmpty(sse.CustomerKey),
			SSECustomerKeyMD5:    nilIfEmpty(sse.CustomerKeyMD5),
		},
	)
	if err != nil {
		return nil, err
	}
	return &s3io.S3GetObjectOutputReader{
		Ctx:          ctx,
		Goo:          goo,
		Log:          s3Bio.Log,
		Lookback:     s3Bio.ReaderLookbackBufferSize,
		MinChunkSize: s3Bio.ReaderMinChunkSize,
	}, nil
}

func (s3Bio *S3BucketIO) Filewrite(req *sftp.Request) (io.WriterAt, error) {
	if !s3Bio.Perms.Writable {
		return nil, fmt.Errorf("write operation not allowed as per configuration")
	}
	sess, err := aws_session.NewSession()
	if err != nil {
		return nil, err
	}
	maxObjectSize := s3Bio.Bucket.MaxObjectSize
	if maxObjectSize < 0 {
		maxObjectSize = int64(^uint(0) >> 1)
	}
	key := buildKey(s3Bio.Bucket, req.Filepath)
	info := &phantomObjects.PhantomObjectInfo{
		Key:          key,
		Size:         0,
		LastModified: s3Bio.Now(),
	}
	s3io.F(s3Bio.Log.Debug, "S3PutObjectWriter.New(key=%s)", key)
	oow := &s3io.S3PutObjectWriter{
		Ctx:                  combineContext(s3Bio.Ctx, req.Context()),
		Bucket:               s3Bio.Bucket.Bucket,
		Key:                  key,
		S3:                   s3Bio.Bucket.S3(sess),
		ServerSideEncryption: s3Bio.ServerSideEncryption,
		Log:                  s3Bio.Log,
		MaxObjectSize:        maxObjectSize,
		PhantomObjectMap:     s3Bio.PhantomObjectMap,
		Info:                 info,
		Writer:               byteswriter.NewBytesWriter(),
	}
	info.Opaque = oow
	s3Bio.PhantomObjectMap.Add(info)
	return oow, nil
}

func (s3Bio *S3BucketIO) Filecmd(req *sftp.Request) error {
	switch req.Method {
	case "Rename":
		if !s3Bio.Perms.Writable {
			return fmt.Errorf("write operation not allowed as per configuration")
		}
		src := buildKey(s3Bio.Bucket, req.Filepath)
		dest := buildKey(s3Bio.Bucket, req.Target)
		if s3Bio.PhantomObjectMap.Rename(src, dest) {
			return nil
		}
		sess, err := aws_session.NewSession()
		if err != nil {
			return err
		}
		srcStr := src.String()
		destStr := dest.String()
		copySource := s3Bio.Bucket.Bucket + "/" + srcStr
		sse := s3Bio.ServerSideEncryption
		s3io.F(s3Bio.Log.Debug, "CopyObject(Bucket=%s, Key=%s, CopySource=%s, Sse=%v)", s3Bio.Bucket.Bucket, destStr, copySource, sse.Type)
		_, err = s3Bio.Bucket.S3(sess).CopyObjectWithContext(
			combineContext(s3Bio.Ctx, req.Context()),
			&aws_s3.CopyObjectInput{
				ACL:                  &aclPrivate,
				Bucket:               &s3Bio.Bucket.Bucket,
				CopySource:           &copySource,
				Key:                  &destStr,
				ServerSideEncryption: sseTypes[sse.Type],
				SSECustomerAlgorithm: nilIfEmpty(sse.CustomerAlgorithm()),
				SSECustomerKey:       nilIfEmpty(sse.CustomerKey),
				SSECustomerKeyMD5:    nilIfEmpty(sse.CustomerKeyMD5),
				SSEKMSKeyId:          nilIfEmpty(sse.KMSKeyId),
			},
		)
		if err != nil {
			s3Bio.Log.Debug("=> ", err)
			return err
		}
		s3io.F(s3Bio.Log.Debug, "DeleteObject(Bucket=%s, Key=%s)", s3Bio.Bucket.Bucket, srcStr)
		_, err = s3Bio.Bucket.S3(sess).DeleteObjectWithContext(
			combineContext(s3Bio.Ctx, req.Context()),
			&aws_s3.DeleteObjectInput{
				Bucket: &s3Bio.Bucket.Bucket,
				Key:    &srcStr,
			},
		)
		if err != nil {
			s3Bio.Log.Debug("=> ", err)
			return err
		}
	case "Remove":
		if !s3Bio.Perms.Writable {
			return fmt.Errorf("write operation not allowed as per configuration")
		}
		key := buildKey(s3Bio.Bucket, req.Filepath)
		if s3Bio.PhantomObjectMap.Remove(key) != nil {
			return nil
		}
		sess, err := aws_session.NewSession()
		if err != nil {
			return err
		}
		keyStr := key.String()
		s3io.F(s3Bio.Log.Debug, "DeleteObject(Bucket=%s, Key=%s)", s3Bio.Bucket.Bucket, key)
		_, err = s3Bio.Bucket.S3(sess).DeleteObjectWithContext(
			combineContext(s3Bio.Ctx, req.Context()),
			&aws_s3.DeleteObjectInput{
				Bucket: &s3Bio.Bucket.Bucket,
				Key:    &keyStr,
			},
		)
		if err != nil {
			s3Bio.Log.Debug("=> ", err)
			return err
		}
	}
	return nil
}

func (s3Bio *S3BucketIO) Filelist(req *sftp.Request) (sftp.ListerAt, error) {

	sess, err := aws_session.NewSession()
	if err != nil {
		return nil, err
	}

	switch req.Method {
	case "Stat", "ReadLink":
		if !s3Bio.Perms.Readable && !s3Bio.Perms.Listable {
			return nil, fmt.Errorf("stat operation not allowed as per configuration")
		}
		key := buildKey(s3Bio.Bucket, req.Filepath)

		lister := s3io.S3ObjectStat{
			DebugLogger:      s3Bio.Log,
			Ctx:              combineContext(s3Bio.Ctx, req.Context()),
			Bucket:           s3Bio.Bucket.Bucket,
			Key:              key,
			S3:               s3Bio.Bucket.S3(sess),
			PhantomObjectMap: s3Bio.PhantomObjectMap,
		}
		return &lister, nil
	case "List":
		if !s3Bio.Perms.Listable {
			return nil, fmt.Errorf("listing operation not allowed as per configuration")
		}

		return &s3io.S3ObjectLister{
			DebugLogger:      s3Bio.Log,
			Ctx:              combineContext(s3Bio.Ctx, req.Context()),
			Bucket:           s3Bio.Bucket.Bucket,
			Prefix:           buildKey(s3Bio.Bucket, req.Filepath),
			S3:               *s3Bio.Bucket.S3(sess),
			Lookback:         s3Bio.ListerLookbackBufferSize,
			PhantomObjectMap: s3Bio.PhantomObjectMap,
		}, nil
	default:
		return nil, fmt.Errorf("unsupported method: %s", req.Method)
	}
}
