// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"log"
	"os"

	s3checksum "amazon-s3-checksum-tool"

	"github.com/urfave/cli/v2"
)

func main() {

	var file string
	var bucket string
	var key string
	var manifestFile string
	var threads int
	var chunksize int64
	var printHex bool
	var region string
	var awsProfile string
	var usePathStyle bool

	//
	app := &cli.App{
		Usage: "CLI utility for S3 concurrent uploads and integrity checking",
		Commands: []*cli.Command{
			{
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:        "file",
						Value:       "",
						Usage:       "file",
						Destination: &file,
					},
					&cli.StringFlag{
						Name:        "manifest",
						Value:       "manifest.json",
						Usage:       "--manifest output.json will generate a json file with all the parts and the checksums so it can be verified later",
						Destination: &manifestFile,
					},
					&cli.Int64Flag{
						Name:        "chunksize",
						Value:       64,
						Usage:       "--chunksize=10 will create 10MB chunks",
						Destination: &chunksize,
					},
					&cli.BoolFlag{
						Name:        "use-path-style",
						Value:       false,
						Usage:       "--use-path-style changes to path-style (old) insteaad of virtual-hosted style (new) s3 hostnames",
						Destination: &usePathStyle,
					},
					&cli.IntFlag{
						Name:        "threads",
						Value:       16,
						Usage:       "--threads=10",
						Destination: &threads,
					},
					&cli.BoolFlag{
						Name:        "print-hex",
						Value:       false,
						Destination: &printHex,
					},
				},
				Name:  "checksum",
				Usage: "checksum",
				Action: func(c *cli.Context) error {
					if printHex {
						s3checksum.PrintHexMode()
					}
					if threads < 0 {
						log.Fatalf("threads must be a positive value. Input value: %d", threads)
					}
					if file == "" {
						return fmt.Errorf("--file flag is required")
					}
					mpf, err := s3checksum.NewMultipartFile(s3checksum.MultipartFileOpts{
						FilePath:         file,
						ManifestFilePath: manifestFile,
						PartSize:         chunksize * 1024 * 1024,
						Threads:          threads,
					})
					if err != nil {
						return err
					}
					info, err := mpf.CalculateChecksum(context.Background())
					if err != nil {
						return err
					}

					for _, part := range info.PartList {
						fmt.Printf("Part: %05d\t\t%s\n", part.PartNumber, part.Checksum)
					}
					fmt.Printf("Amazon S3 SHA256:\t%s-%d\n", info.Checksum, len(info.PartList))
					fmt.Printf("Amazon S3 Etag:\t%x-%d\n", info.Etag, len(info.PartList))
					return nil
				},
			},
			{
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:        "bucket",
						Value:       "",
						Usage:       "bucket",
						Destination: &bucket,
					},
					&cli.StringFlag{
						Name:        "key",
						Value:       "",
						Usage:       "key",
						Destination: &key,
					},
					&cli.StringFlag{
						Name:        "file",
						Value:       "",
						Usage:       "file",
						Destination: &file,
					},
					&cli.StringFlag{
						Name:        "manifest",
						Value:       "manifest.json",
						Usage:       "--manifest output.json will generate a json file with all the parts and the checksums so it can be verified later",
						Destination: &manifestFile,
					},
					&cli.IntFlag{
						Name:        "threads",
						Value:       16,
						Usage:       "--threads=10",
						Destination: &threads,
					},
					&cli.Int64Flag{
						Name:        "chunksize",
						Value:       64,
						Usage:       "--chunksize=10 will create 10MB chunks",
						Destination: &chunksize,
					},
					&cli.BoolFlag{
						Name:        "use-path-style",
						Value:       false,
						Usage:       "--use-path-style changes to path-style (old) insteaad of virtual-hosted style (new) s3 hostnames",
						Destination: &usePathStyle,
					},
					&cli.StringFlag{
						Name:        "region",
						Value:       "us-west-2",
						Usage:       "region",
						Destination: &region,
					},
					&cli.StringFlag{
						Name:        "profile",
						Value:       "",
						Usage:       "",
						Destination: &awsProfile,
					},
				},
				Name:  "upload",
				Usage: "upload",
				Action: func(c *cli.Context) error {

					if file == "" {
						return fmt.Errorf("--file flag is required")
					}

					return s3checksum.Upload(context.Background(), &s3checksum.UploadOptions{
						Bucket:       bucket,
						Key:          key,
						NumRoutines:  threads,
						LocalFile:    file,
						ManifestFile: manifestFile,
						PartSize:     chunksize * 1024 * 1024,
						Region:       region,
						AWSProfile:   awsProfile,
						UsePathStyle: usePathStyle,
					})
				},
			},
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}

}
