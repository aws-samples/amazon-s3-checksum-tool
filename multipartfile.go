// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package s3checksum

import (
	"context"
	"crypto/md5"
	"crypto/sha256"
	"fmt"
	"hash"
	"io"
	"log"
	"math"
	"os"
	"sort"
	"sync"
)

const (
	MIN_PART_SIZE = 5242880
)

type MultipartFileOpts struct {
	FilePath         string
	ManifestFilePath string
	FileSize         int64
	NumberOfParts    int
	PartSize         int64
	NumRoutines      int
	HashFun          func() hash.Hash
	Threads          int
	Algorithm        string
}

type MultipartFile struct {
	MultipartFileOpts
	HashName    string
	bufferPool  *sync.Pool
	hashPool    *sync.Pool
	md5HashPool *sync.Pool
}

func NewMultipartFile(options MultipartFileOpts, optFns ...func(*MultipartFileOpts)) (*MultipartFile, error) {

	options = options.Copy()

	checkRequiredArgs(&options)

	for _, fn := range optFns {
		fn(&options)
	}

	resolvePartSize(&options)

	bufferPool := &sync.Pool{
		New: func() interface{} {
			return make([]byte, options.PartSize)
		},
	}
	hashPool := &sync.Pool{
		New: func() interface{} {
			return options.HashFun()
		},
	}

	md5HashPool := &sync.Pool{
		New: func() interface{} {
			return md5.New()
		},
	}

	return &MultipartFile{
		MultipartFileOpts: options,
		bufferPool:        bufferPool,
		hashPool:          hashPool,
		md5HashPool:       md5HashPool,
	}, nil
}

func (o MultipartFileOpts) Copy() MultipartFileOpts {
	to := o
	return to
}

func resolvePartSize(o *MultipartFileOpts) {
	// size option must be already defined
	if o.FileSize == 0 {
		log.Fatal("file size cannot be 0")
	}

	if o.PartSize < MIN_PART_SIZE {
		log.Fatal("part size should be larger than 5MB")
	}

	NumberOfParts := float64(o.FileSize) / float64(o.PartSize)
	o.NumberOfParts = int(math.Ceil(NumberOfParts))

}

func (m *MultipartFile) calculateEtag(data []byte) []byte {
	mh := m.md5HashPool.Get().(hash.Hash)
	defer m.md5HashPool.Put(mh)
	mh.Reset()
	mh.Write(data)
	return mh.Sum(nil)
}

func (m *MultipartFile) CalculateChecksumForPart(ctx context.Context, partNum int32) (*PartInfo, error) {

	start := (m.PartSize * int64(partNum))
	end := start + m.PartSize
	if end > m.FileSize {
		end = m.FileSize
	}
	size := end - start

	f, err := os.Open(m.FilePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	_, err = f.Seek(start, 0)
	if err != nil {
		return nil, err
	}

	// Get from the buffer pool so we're not re-allocating
	buffer := m.bufferPool.Get()
	defer m.bufferPool.Put(buffer)
	poolData := buffer.([]byte)
	poolData = poolData[0:size]

	n, err := io.ReadFull(f, poolData)
	if err != nil && err != io.EOF {
		return nil, err
	}
	if int64(n) != size {
		err = fmt.Errorf("limitedReader returned %d bytes instead of the expected %d bytes", n, size)
		return nil, err
	}
	data := poolData[:n]

	// Calculate the user requested hash
	h := m.hashPool.Get().(hash.Hash)
	defer m.hashPool.Put(h)
	h.Reset()
	h.Write(data)
	checksum := h.Sum(nil)

	md5checksum := m.calculateEtag(data)

	p := &PartInfo{
		PartNumber:  partNum + 1,
		Size:        size,
		Checksum:    checksum[:],
		Algorithm:   "sha256", // allow the user to change the algorithm
		MD5Checksum: md5checksum[:],
	}
	return p, nil
}

type ChecksumResult struct {
	Info *PartInfo
	Err  error
}

func (m *MultipartFile) CalculateChecksum(ctx context.Context) (*ManifestFile, error) {

	results := make(chan ChecksumResult)
	limiter := make(chan struct{}, m.Threads)
	partInfoList := []*PartInfo{}

	wg := sync.WaitGroup{}
	wg.Add(m.NumberOfParts)

	go func() {
		for i := int32(0); i < int32(m.NumberOfParts); i++ {
			limiter <- struct{}{}
			go func(i int32) {
				defer wg.Done()
				partInfo, err := m.CalculateChecksumForPart(ctx, i)
				if err != nil {
					log.Fatal(err.Error())
				}
				<-limiter
				results <- ChecksumResult{partInfo, err}
			}(i)
		}
	}()

	go func() {
		wg.Wait()
		close(results)
		close(limiter)
	}()

	for m := range results {
		if m.Err != nil {
			fmt.Printf("Error calculating checksum for %d\n%s", m.Info.PartNumber, m.Err.Error())
			m.Info.Checksum = []byte("ERROR CALCULATING")
		}
		partInfoList = append(partInfoList, m.Info)
	}

	sort.Slice(partInfoList, func(i, j int) bool {
		return partInfoList[i].PartNumber < partInfoList[j].PartNumber
	})

	var manifest *ManifestFile
	if len(partInfoList) > 1 {
		h := m.hashPool.Get().(hash.Hash)
		defer m.hashPool.Put(h)
		h.Reset()

		etagChecksum := m.md5HashPool.Get().(hash.Hash)
		defer m.md5HashPool.Put(etagChecksum)
		etagChecksum.Reset()

		for _, part := range partInfoList {
			h.Write(part.Checksum)
			etagChecksum.Write(part.MD5Checksum)
		}
		checksum := ByteSlice(h.Sum(nil))
		etag := etagChecksum.Sum(nil)

		manifest = &ManifestFile{
			PartList: partInfoList,
			// Algorithm: m.ChecksumAlgorithm, TODO,
			Etag:     etag,
			Checksum: checksum,
		}
	} else {
		manifest = &ManifestFile{
			Etag:     partInfoList[0].MD5Checksum,
			Checksum: partInfoList[0].Checksum,
		}
	}
	manifest.Filename = m.FilePath
	manifest.PartSize = int(m.PartSize)
	manifest.Algorithm = m.Algorithm

	var err error
	if m.ManifestFilePath != "" {
		mf := []*ManifestFile{manifest}
		err = WriteSimpleManifest(m.ManifestFilePath, mf)
		if err != nil {
			log.Printf("error writing manifest file\n%s", err.Error())
		}
	}

	return manifest, err
}

func checkRequiredArgs(o *MultipartFileOpts) {
	if o.FilePath == "" {
		log.Fatal("FilePath is a required parameter")
	}

	fileInfo, err := os.Stat(o.FilePath)
	if err != nil {
		log.Fatal(err.Error())
	}
	o.FileSize = fileInfo.Size()
	o.NumRoutines = 16

	if o.HashFun == nil {
		o.HashFun = sha256.New
	}
}
