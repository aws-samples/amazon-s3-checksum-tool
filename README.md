## amazon-s3-checksum-tool

This tool is for calculating the Etag and/or additional checksums (currently only SHA256) for Amazon S3 Multipart Uploads. 

### Build

```bash
go build ./cmd/s3checksum
```

### Usage

There are two functionalities built into the application: upload and checksum. 

**Upload** uses the AWS Go SDK Transfer Manager to concurrently upload an Amazon S3 MultiPartUpload and use Trailing Checksums. It currently supports SHA256. 

**Checksum** will perform a checksum on a local file and provide the individual checksums across every part of the MultiPart object. This allows you to compare your file locally to the one uploaded to Amazon S3. It also prints the checksum-of-checksums value. 

Both functions require a --chunksize argument to determine the PartSize (provided in Megabytes)

```bash
NAME:
   s3checksum - CLI Utility for S3 concurrent uploads and integrity checking

USAGE:
   s3checksum [global options] command [command options] [arguments...]

COMMANDS:
   checksum  checksum
   upload    upload
   help, h   Shows a list of commands or help for one command

GLOBAL OPTIONS:
   --help, -h  show help (default: false)
```

### Examples

#### Upload example
```
s3checksum upload --file /Users/myuser/Documents/LargeFile.tar --bucket=my-bucket --key=my-folder/LargeFile.tar --chunksize=10
```

#### Checksum example

```bash
$ s3checksum checksum --file /Users/myuser/Documents/LargeFile.tar --chunksize=10

Part: 00001		6vxOyaaz7pH1XQGKYFBXw5vmM8TvV7mbt6rdtUjke0w=
Part: 00002		KpYsnUUtOkv6M7Yvtpft5ZPPQbo+L2Pt80eMnWWwUTA=
Part: 00003		TLshsT+eJV2A99owiaBFL6xbAYIh0YpqlDuj0CLwvhs=
Part: 00004		mcJrgo+3CffgdBcWInozbC4MUUuw2mfjhTsN5mCj5bM=
Part: 00005		v3pt2ocOFfPoOWM5UnbuW6JcJIV6613VEynckY+jgqk=
Part: 00006		zE+LKfBmPg9ZORZ9XHaebsnd9r4V/yXChV9kKQ187Gw=
Part: 00007		5Tt8QVIQu0E4acnvWEds59fNdVDOSuyxC6PG3cMMATg=
Amazon S3 Checksum:	nePidYIsjC+4h/W+3zhFQNXpjPCPs8D038C0v/U1a8A=-7
```