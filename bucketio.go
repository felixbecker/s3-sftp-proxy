package main

import (
	"s3-sftp-proxy/config"

	"time"

	"github.com/aws/aws-sdk-go/aws"
	// s3crypto "github.com/aws/aws-sdk-go/service/s3/s3crypto"
)

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
