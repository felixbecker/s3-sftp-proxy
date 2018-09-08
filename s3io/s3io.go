package s3io

import (
	"fmt"
	"os"
	"s3-sftp-proxy/config"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

var aclPrivate = "private"

var sseTypes = map[config.ServerSideEncryptionType]*string{
	config.ServerSideEncryptionTypeKMS: aws.String("aws:kms"),
}

func aclToMode(owner *s3.Owner, grants []*s3.Grant) os.FileMode {
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
