package s3io

import (
	"bytes"
	"context"
	"fmt"
	"s3-sftp-proxy/byteswriter"
	"s3-sftp-proxy/config"
	"s3-sftp-proxy/logging"
	"s3-sftp-proxy/phantomObjects"
	"s3-sftp-proxy/s3path"
	"sync"

	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
)

type S3PutObjectWriter struct {
	Ctx                  context.Context
	Bucket               string
	Key                  s3path.Path
	S3                   *aws_s3.S3
	ServerSideEncryption *config.ServerSideEncryptionConfig
	Log                  interface {
		logging.DebugLogger
		logging.ErrorLogger
	}
	MaxObjectSize    int64
	Info             *phantomObjects.PhantomObjectInfo
	PhantomObjectMap *phantomObjects.PhantomObjectMap
	mtx              sync.Mutex
	Writer           *byteswriter.BytesWriter
}

func (oow *S3PutObjectWriter) Close() error {
	F(oow.Log.Debug, "S3PutObjectWriter.Close")
	oow.mtx.Lock()
	defer oow.mtx.Unlock()
	phInfo := oow.Info.GetOne()
	oow.PhantomObjectMap.RemoveByInfoPtr(oow.Info)
	key := phInfo.Key.String()
	sse := oow.ServerSideEncryption
	F(oow.Log.Debug, "PutObject(Bucket=%s, Key=%s, Sse=%v)", oow.Bucket, key, sse)
	_, err := oow.S3.PutObject(
		&aws_s3.PutObjectInput{
			ACL:                  &aclPrivate,
			Body:                 bytes.NewReader(oow.Writer.Bytes()),
			Bucket:               &oow.Bucket,
			Key:                  &key,
			ServerSideEncryption: sseTypes[sse.Type],
			SSECustomerAlgorithm: nilIfEmpty(sse.CustomerAlgorithm()),
			SSECustomerKey:       nilIfEmpty(sse.CustomerKey),
			SSECustomerKeyMD5:    nilIfEmpty(sse.CustomerKeyMD5),
			SSEKMSKeyId:          nilIfEmpty(sse.KMSKeyId),
		},
	)

	if err != nil {
		oow.Log.Debug("=> ", err)
		F(oow.Log.Error, "failed to put object: %s", err.Error())
	} else {
		oow.Log.Debug("=> OK")
	}
	return nil
}

func (oow *S3PutObjectWriter) WriteAt(buf []byte, off int64) (int, error) {
	oow.mtx.Lock()
	defer oow.mtx.Unlock()
	if oow.MaxObjectSize >= 0 {
		if int64(len(buf))+off > oow.MaxObjectSize {
			return 0, fmt.Errorf("file too large: maximum allowed size is %d bytes", oow.MaxObjectSize)
		}
	}
	F(oow.Log.Debug, "len(buf)=%d, off=%d", len(buf), off)
	n, err := oow.Writer.WriteAt(buf, off)
	oow.Info.SetSize(oow.Writer.Size())
	return n, err
}
