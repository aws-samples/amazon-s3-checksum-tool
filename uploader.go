// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package s3checksum

import (
	"context"
	"encoding/base64"
	"fmt"
	"log"
	"os"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type UploadOptions struct {
	Bucket       string
	Key          string
	LocalFile    string
	ManifestFile string
	NumRoutines  int
	PartSize     int64
	Region       string
	AWSProfile   string
	UsePathStyle bool
}

func Upload(ctx context.Context, opts *UploadOptions) error {
	optFns := []func(*config.LoadOptions) error{
		config.WithRegion(opts.Region),
	}
	if opts.AWSProfile != "" {
		optFns = append(optFns, config.WithSharedConfigProfile(opts.AWSProfile))

	}
	cfg, err := config.LoadDefaultConfig(context.TODO(), optFns...)
	if err != nil {
		log.Fatal(err.Error())
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = opts.UsePathStyle
	})

	f, err := os.Open(opts.LocalFile)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	if opts.NumRoutines == 0 {
		opts.NumRoutines = 16
	}

	uploader := manager.NewUploader(client, func(u *manager.Uploader) {
		u.PartSize = opts.PartSize
		u.Concurrency = opts.NumRoutines
	})

	log.Println("Beginning upload...")
	uploadOutput, err := uploader.Upload(ctx, &s3.PutObjectInput{
		ChecksumAlgorithm: types.ChecksumAlgorithmSha256, // Trailing Checksum
		Bucket:            &opts.Bucket,
		Key:               &opts.Key,
		Body:              f,
	})

	if err != nil {
		return err
	}

	parts := []*PartInfo{}
	for _, p := range uploadOutput.CompletedParts {
		c, err := base64.StdEncoding.DecodeString(*p.ChecksumSHA256)
		if err != nil {
			log.Printf("unable to decode checksum")
		}
		pi := &PartInfo{
			PartNumber: *p.PartNumber,
			Checksum:   ByteSlice(c),
			Algorithm:  "sha256",
		}
		fmt.Printf("Part: %05d\t\t%s\n", pi.PartNumber, pi.Checksum)
		parts = append(parts, pi)
	}

	etag, err := convertS3EtagToBytes(*uploadOutput.ETag)
	if err != nil {
		return err
	}

	if opts.ManifestFile != "" {
		m := &ManifestFile{
			PartList:  parts,
			Algorithm: "sha256",
			Etag:      etag,
		}
		mf := []*ManifestFile{m}
		if err := WriteSimpleManifest(opts.ManifestFile, mf); err != nil {
			log.Printf("failed writing manifest at: %s", opts.ManifestFile)
		}
	}
	fmt.Printf("Amazon S3 SHA256:\t%s\n", *uploadOutput.ChecksumSHA256)

	etagstr := fmt.Sprintf("%x", etag)
	if len(parts) > 0 {
		etagstr = fmt.Sprintf("%s-%d", etagstr, len(parts))
	}

	fmt.Printf("Amazon S3 Etag:\t%s\n", etagstr)

	return nil

}
