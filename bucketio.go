package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"s3-sftp-proxy/byteswriter"
	"s3-sftp-proxy/s3path"

	"s3-sftp-proxy/config"
	"s3-sftp-proxy/logging"

	"s3-sftp-proxy/phantomObjects"
	"s3-sftp-proxy/s3io"
	"sync"
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

type ReadDeadlineSettable interface {
	SetReadDeadline(t time.Time) error
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

type S3GetObjectOutputReader struct {
	Ctx          context.Context
	Goo          *aws_s3.GetObjectOutput
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

func (oor *S3GetObjectOutputReader) ReadAt(buf []byte, off int64) (int, error) {
	oor.mtx.Lock()
	defer oor.mtx.Unlock()

	s3io.F(oor.Log.Debug, "len(buf)=%d, off=%d", len(buf), off)
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

	s3io.F(oor.Log.Debug, "s=%d, len(oor.spooled)=%d, oor.Lookback=%d", s, len(oor.spooled), oor.Lookback)
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

type S3ObjectLister struct {
	logging.DebugLogger
	Ctx              context.Context
	Bucket           string
	Prefix           s3path.Path
	S3               *aws_s3.S3
	Lookback         int
	PhantomObjectMap *phantomObjects.PhantomObjectMap
	spoolOffset      int
	spooled          []os.FileInfo
	continuation     *string
	noMore           bool
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
		sol.spooled = append(sol.spooled, s3io.NewObjectFileInfo(".", time.Unix(1, 0), 0, 0755|os.ModeDir))
		sol.spooled = append(sol.spooled, s3io.NewObjectFileInfo("..", time.Unix(1, 0), 0, 0755|os.ModeDir))

		phObjs := sol.PhantomObjectMap.List(sol.Prefix)
		for _, phInfo := range phObjs {
			_phInfo := phInfo.GetOne()
			sol.spooled = append(sol.spooled, s3io.NewObjectFileInfo(_phInfo.Key.Base(), _phInfo.LastModified, _phInfo.Size, 0600 /* TODO*/))
		}
	}

	prefix := sol.Prefix.String()
	if prefix != "" {
		prefix += "/"
	}
	s3io.F(sol.Debug, "ListObjectsV2WithContext(Bucket=%s, Prefix=%s, Continuation=%v)", sol.Bucket, prefix, sol.continuation)
	out, err := sol.S3.ListObjectsV2WithContext(
		sol.Ctx,
		&aws_s3.ListObjectsV2Input{
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
	s3io.F(sol.Debug, "=> { CommonPrefixes=len(%d), Contents=len(%d) }", len(out.CommonPrefixes), len(out.Contents))

	if sol.continuation == nil {
		for _, cPfx := range out.CommonPrefixes {

			sol.spooled = append(sol.spooled, s3io.NewObjectFileInfo(path.Base(*cPfx.Prefix), time.Unix(1, 0), 0, 0755|os.ModeDir))
		}
	}
	for _, obj := range out.Contents {
		// if *obj.Key == sol.Prefix {
		// 	continue
		// }

		sol.spooled = append(sol.spooled, s3io.NewObjectFileInfo(
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

type S3ObjectStat struct {
	logging.DebugLogger
	Ctx              context.Context
	Bucket           string
	Key              s3path.Path
	S3               *aws_s3.S3
	PhantomObjectMap *phantomObjects.PhantomObjectMap
}

func (sos *S3ObjectStat) ListAt(result []os.FileInfo, o int64) (int, error) {
	s3io.F(sos.Debug, "S3ObjectStat.ListAt: len(result)=%d offset=%d", len(result), o)
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
		result[0] = s3io.NewObjectFileInfo(
			"/",
			time.Time{},
			0,
			0755|os.ModeDir,
		)
	} else {
		phInfo := sos.PhantomObjectMap.Get(sos.Key)
		if phInfo != nil {
			_phInfo := phInfo.GetOne()
			result[0] = s3io.NewObjectFileInfo(
				_phInfo.Key.Base(),
				_phInfo.LastModified,
				_phInfo.Size,
				0600, // TODO
			)
		} else {
			key := sos.Key.String()
			s3io.F(sos.Debug, "GetObjectAclWithContext(Bucket=%s, Key=%s)", sos.Bucket, key)
			out, err := sos.S3.GetObjectAclWithContext(
				sos.Ctx,
				&aws_s3.GetObjectAclInput{
					Bucket: &sos.Bucket,
					Key:    &key,
				},
			)
			if err == nil {
				s3io.F(sos.Debug, "=> %v", out)
				s3io.F(sos.Debug, "HeadObjectWithContext(Bucket=%s, Key=%s)", sos.Bucket, key)
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

				var objInfo *s3io.ObjectFileInfo
				if err != nil {
					s3io.F(sos.Debug, "=> { ContentLength=%d, LastModified=%v, Error=%+v}", *headOut.ContentLength, *headOut.LastModified, err)
					objInfo = s3io.NewObjectFileInfo(sos.Key.Base(), time.Now(), 0, aclToMode(out.Owner, out.Grants))
				} else {
					s3io.F(sos.Debug, "=> { ContentLength=%d, LastModified=%v }", *headOut.ContentLength, *headOut.LastModified)
					objInfo = s3io.NewObjectFileInfo(sos.Key.Base(), *headOut.LastModified, *headOut.ContentLength, aclToMode(out.Owner, out.Grants))
				}

				result[0] = objInfo
			} else {
				sos.Debug("=> ", err)
				s3io.F(sos.Debug, "ListObjectsV2WithContext(Bucket=%s, Prefix=%s)", sos.Bucket, key)
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
				s3io.F(sos.Debug, "=> { CommonPrefixes=len(%d), Contents=len(%d) }", len(out.CommonPrefixes), len(out.Contents))

				result[0] = s3io.NewObjectFileInfo(sos.Key.Base(), time.Now(), 0, 0755|os.ModeDir)

			}
		}
	}
	return 1, nil
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

func (s3io *S3BucketIO) Filelist(req *sftp.Request) (sftp.ListerAt, error) {
	sess, err := aws_session.NewSession()
	if err != nil {
		return nil, err
	}
	switch req.Method {
	case "Stat", "ReadLink":
		if !s3io.Perms.Readable && !s3io.Perms.Listable {
			return nil, fmt.Errorf("stat operation not allowed as per configuration")
		}
		key := buildKey(s3io.Bucket, req.Filepath)
		return &S3ObjectStat{
			DebugLogger:      s3io.Log,
			Ctx:              combineContext(s3io.Ctx, req.Context()),
			Bucket:           s3io.Bucket.Bucket,
			Key:              key,
			S3:               s3io.Bucket.S3(sess),
			PhantomObjectMap: s3io.PhantomObjectMap,
		}, nil
	case "List":
		if !s3io.Perms.Listable {
			return nil, fmt.Errorf("listing operation not allowed as per configuration")
		}
		return &S3ObjectLister{
			DebugLogger:      s3io.Log,
			Ctx:              combineContext(s3io.Ctx, req.Context()),
			Bucket:           s3io.Bucket.Bucket,
			Prefix:           buildKey(s3io.Bucket, req.Filepath),
			S3:               s3io.Bucket.S3(sess),
			Lookback:         s3io.ListerLookbackBufferSize,
			PhantomObjectMap: s3io.PhantomObjectMap,
		}, nil
	default:
		return nil, fmt.Errorf("unsupported method: %s", req.Method)
	}
}

func IsEOF(e error) bool {
	return e == io.EOF || e == io.ErrUnexpectedEOF
}
