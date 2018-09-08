package config

import (
	"fmt"
	"strings"
)

type ServerSideEncryptionType int

const (
	ServerSideEncryptionTypeNone = iota
	ServerSideEncryptionTypeAES256
	ServerSideEncryptionTypeKMS
)

var sseNameToEnumMap = map[string]ServerSideEncryptionType{
	"":       ServerSideEncryptionTypeNone,
	"none":   ServerSideEncryptionTypeNone,
	"aes256": ServerSideEncryptionTypeAES256,
	"kms":    ServerSideEncryptionTypeKMS,
}

func (v *ServerSideEncryptionType) UnmarshalText(text []byte) error {
	_v, ok := sseNameToEnumMap[strings.ToLower(string(text))]
	if !ok {
		return fmt.Errorf("invalid value for ServerSideEncryption: %s", string(text))
	}
	*v = _v
	return nil
}

type ServerSideEncryptionConfig struct {
	Type           ServerSideEncryptionType
	CustomerKey    string
	CustomerKeyMD5 string
	KMSKeyId       string
}

func (cfg *ServerSideEncryptionConfig) CustomerAlgorithm() string {
	if cfg.Type == ServerSideEncryptionTypeAES256 {
		return "AES256"
	}
	return ""

}
