// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package s3checksum

import (
	"encoding/base64"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
)

var (
	printHex = false
)

// PrintHexMode sets the CLI to print checksums in hex instead of base64
func PrintHexMode() {
	printHex = true
}

type PartInfo struct {
	PartNumber  int32     `json:"part_number"`
	Size        int64     `json:"size"`
	Algorithm   string    `json:"algorithm"`
	Checksum    ByteSlice `json:"checksum"`
	MD5Checksum []byte    `json:""`
}

type ManifestFile struct {
	Filename  string      `json:"filename"`
	PartSize  int         `json:"part_size"`
	PartList  []*PartInfo `json:"part_list"`
	Checksum  ByteSlice   `json:"checksum"`
	Etag      []byte      `json:"Etag"`
	Algorithm string      `json:"algorithm"`
}

type ObjectAttributes struct {
	Filename  string    `json:"filename"`
	PartSize  int       `json:"part_size"`
	Algorithm string    `json:"algorithm"`
	Checksum  ByteSlice `json:"checksum"`
	Etag      []byte    `json:"Etag"`
}

type ByteSlice []byte

func (m ByteSlice) MarshalJSON() ([]byte, error) {
	return json.Marshal(hex.EncodeToString(m))
}
func (m *ByteSlice) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}

	n, err := hex.DecodeString(s)
	if err != nil {
		return err
	}
	*m = n

	return nil
}

func (m ByteSlice) String() string {
	if printHex {
		return hex.EncodeToString(m)
	} else {
		return base64.StdEncoding.EncodeToString(m)
	}

}

// WriteSimpleManifest is a simplified CSV that doesn't include part checksums,
// only checksum of checksums.
func WriteSimpleManifest(path string, mf []*ManifestFile) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	rows := [][]string{}
	for _, v := range mf {
		partSize := fmt.Sprintf("%d", v.PartSize)
		checksumOfChecksums := fmt.Sprintf("%s-%d", v.Checksum.String(), len(v.PartList))
		etag := fmt.Sprintf("%x-%d", v.Etag, len(v.PartList))

		rows = append(rows, []string{
			v.Filename,
			partSize,
			v.Algorithm,
			checksumOfChecksums,
			etag,
		})
	}

	return csv.NewWriter(f).WriteAll(rows)
}
