package s3io

import (
	"fmt"
	"io"
	"s3-sftp-proxy/config"

	"github.com/aws/aws-sdk-go/aws"
)

var aclPrivate = "private"

var sseTypes = map[config.ServerSideEncryptionType]*string{
	config.ServerSideEncryptionTypeKMS: aws.String("aws:kms"),
}

func IsEOF(e error) bool {
	return e == io.EOF || e == io.ErrUnexpectedEOF
}

func nilIfEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

type PrintlnLike func(...interface{})

func F(p PrintlnLike, f string, args ...interface{}) {
	p(fmt.Sprintf(f, args...))
}
