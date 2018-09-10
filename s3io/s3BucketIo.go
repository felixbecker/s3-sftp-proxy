package s3io

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"s3-sftp-proxy/byteswriter"
	"s3-sftp-proxy/config"
	"s3-sftp-proxy/logging"
	"s3-sftp-proxy/phantomObjects"

	"time"

	"github.com/pkg/sftp"
)

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
		return bytes.NewReader(phInfo.Opaque.(*S3PutObjectWriter).Writer.Bytes()), nil
	}

	keyStr := key.String()
	ctx := combineContext(s3Bio.Ctx, req.Context())
	F(s3Bio.Log.Debug, "GetObject(Bucket=%s, Key=%s)", s3Bio.Bucket.Bucket, keyStr)
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
	return &S3GetObjectOutputReader{
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
	F(s3Bio.Log.Debug, "S3PutObjectWriter.New(key=%s)", key)
	oow := &S3PutObjectWriter{
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
		F(s3Bio.Log.Debug, "CopyObject(Bucket=%s, Key=%s, CopySource=%s, Sse=%v)", s3Bio.Bucket.Bucket, destStr, copySource, sse.Type)
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
		F(s3Bio.Log.Debug, "DeleteObject(Bucket=%s, Key=%s)", s3Bio.Bucket.Bucket, srcStr)
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
		F(s3Bio.Log.Debug, "DeleteObject(Bucket=%s, Key=%s)", s3Bio.Bucket.Bucket, key)
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

		lister := S3ObjectStat{
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

		return &S3ObjectLister{
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
