// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package s3checksum

import (
	"encoding/hex"
	"regexp"
)

var (
	extractS3 = regexp.MustCompile(`s3:\/\/(.[^\/]*)\/(.*)`)
	hexExp    = regexp.MustCompile(`[0-9A-Fa-f]+`)
)

func ExtractBucketAndPath(s3url string) (bucket string, path string) {
	parts := extractS3.FindAllStringSubmatch(s3url, -1)
	if len(parts) > 0 && len(parts[0]) > 2 {
		bucket = parts[0][1]
		path = parts[0][2]
	}
	return
}

func convertS3EtagToBytes(s string) ([]byte, error) {
	etagstr := hexExp.FindString(s)
	return hex.DecodeString(etagstr)
}
