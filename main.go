package main

import (
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/defaults"
	"github.com/aws/aws-sdk-go/service/s3"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

type Bucket struct {
	Name   string
	Prefix string
	Client *s3.S3
}

type Object struct {
	Key  string
	Size uint64
}

func NewBucket(bucketUrl string, region string) (*Bucket, error) {
	u, err := url.Parse(bucketUrl)

	if err != nil {
		return nil, err
	}

	if u.Scheme != "s3" {
		return nil, fmt.Errorf("Invalid S3 URL scheme: %s", u.Scheme)
	}

	if region == "" {
		req := s3.GetBucketLocationInput{
			Bucket: aws.String(u.Host),
		}

		res, err := s3.New(nil).GetBucketLocation(&req)

		if err != nil {
			return nil, fmt.Errorf("Error while retrieving bucket location: %s", err)
		}

		if res.LocationConstraint != nil {
			region = *res.LocationConstraint
		} else {
			region = "us-east-1"
		}
	}

	client := s3.New(aws.NewConfig().WithRegion(region))

	prefix := u.Path[1:]

	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	return &Bucket{
		Name:   u.Host,
		Prefix: prefix,
		Client: client,
	}, nil
}

func listBucket(bucket *Bucket, out chan *Object) error {
	defer close(out)

	listRequest := s3.ListObjectsInput{
		Bucket: aws.String(bucket.Name),
		Prefix: aws.String(bucket.Prefix),
	}

	callback := func(result *s3.ListObjectsOutput, lastPage bool) bool {
		for _, obj := range result.Contents {
			out <- &Object{
				Key:  (*obj.Key)[len(bucket.Prefix):],
				Size: uint64(*obj.Size),
			}
		}

		return true
	}

	return bucket.Client.ListObjectsPages(&listRequest, callback)
}

func copyObjects(sourceBucket, destBucket *Bucket, objects <-chan *Object) error {
	const concurrency = 100
	copyErrors := make(chan error, concurrency)
	wg := sync.WaitGroup{}

	worker := func() error {
		defer wg.Done()

		for object := range objects {
			if err := copyObject(sourceBucket, destBucket, object); err != nil {
				return fmt.Errorf("Error while copying key %s: %s", object.Key, err)
			}
		}

		return nil
	}

	wg.Add(concurrency)

	for i := 0; i < concurrency; i++ {
		go func() {
			copyErrors <- worker()
		}()
	}

	wg.Wait()

	var err error

	for i := 0; i < concurrency; i++ {
		if copyError := <-copyErrors; copyError != nil {
			err = copyError
		}
	}

	return err
}

func copyObject(sourceBucket, destBucket *Bucket, object *Object) error {
	const tooBigForSimpleCopy = 50 * 1024 * 1024 // bytes

	var err error

	if !*dryRun {
		if object.Size < tooBigForSimpleCopy {
			err = copyObjectSimple(sourceBucket, destBucket, object)
		} else {
			err = copyObjectMultipart(sourceBucket, destBucket, object)
		}
	}

	log.Printf("COPY   %s", object.Key)

	return err
}

func copyObjectSimple(sourceBucket, destBucket *Bucket, object *Object) error {
	sourceKey := sourceBucket.Prefix + object.Key
	req := s3.CopyObjectInput{
		Bucket:     aws.String(destBucket.Name),
		CopySource: aws.String(sourceBucket.Name + "/" + sourceKey),
		Key:        aws.String(destBucket.Prefix + object.Key),
	}

	if _, err := destBucket.Client.CopyObject(&req); err != nil {
		return err
	}

	return nil
}

func copyObjectMultipart(sourceBucket, destBucket *Bucket, object *Object) (err error) {
	const copyPartSize = 50 * 1024 * 1024

	sourceKey := sourceBucket.Prefix + object.Key
	destKey := destBucket.Prefix + object.Key

	// First HEAD the object to retrieve the metadata
	headReq := s3.HeadObjectInput{
		Bucket: aws.String(sourceBucket.Name),
		Key:    aws.String(sourceKey),
	}

	headRes, err := sourceBucket.Client.HeadObject(&headReq)

	if err != nil {
		return fmt.Errorf("Error while retrieving object metadata: %s", err)
	}

	req := s3.CreateMultipartUploadInput{
		Bucket:       aws.String(destBucket.Name),
		Key:          aws.String(destKey),
		ContentType:  headRes.ContentType,
		Metadata:     headRes.Metadata,
		StorageClass: headRes.StorageClass,
	}

	res, err := destBucket.Client.CreateMultipartUpload(&req)

	if err != nil {
		return fmt.Errorf("Error while starting multipart upload: %s", err)
	}

	// Multipart upload is started, from now on, if an error happens, try to
	// abort it
	defer func() {
		if err != nil {
			log.Printf("Error while copying key %s: %s. Aborting multipart upload", destKey, err)

			req := s3.AbortMultipartUploadInput{
				Bucket:   aws.String(destBucket.Name),
				Key:      aws.String(destKey),
				UploadId: res.UploadId,
			}

			if _, err := destBucket.Client.AbortMultipartUpload(&req); err != nil {
				log.Printf("Error while aborting multipart copy for key %s: %s", destKey, err)
			}
		}
	}()

	startRange := uint64(0)
	completedParts := []*s3.CompletedPart{}

	nextRange := func() string {
		endRange := startRange + copyPartSize

		if endRange >= object.Size {
			endRange = object.Size - 1
		}

		rangeString := fmt.Sprintf("bytes=%d-%d", startRange, endRange)
		startRange = endRange + 1

		return rangeString
	}

	copyReq := s3.UploadPartCopyInput{
		Bucket:     aws.String(destBucket.Name),
		Key:        aws.String(destKey),
		CopySource: aws.String(sourceBucket.Name + "/" + sourceKey),
		UploadId:   res.UploadId,
	}

	for startRange < object.Size {
		partNumber := int64(1 + len(completedParts))
		copyReq.CopySourceRange = aws.String(nextRange())
		copyReq.PartNumber = aws.Int64(partNumber)

		res, err := destBucket.Client.UploadPartCopy(&copyReq)

		if err != nil {
			return fmt.Errorf("Error while copying part: %s", err)
		}

		completedPart := s3.CompletedPart{
			PartNumber: aws.Int64(partNumber),
			ETag:       res.CopyPartResult.ETag,
		}

		completedParts = append(completedParts, &completedPart)
	}

	completeReq := s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(destBucket.Name),
		Key:      aws.String(destKey),
		UploadId: res.UploadId,
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: completedParts,
		},
	}

	if _, err := destBucket.Client.CompleteMultipartUpload(&completeReq); err != nil {
		return fmt.Errorf("Error while completing copy: %s", err)
	}

	return nil
}

func deleteObjects(bucket *Bucket, objects <-chan *Object) error {
	buffer := make([]string, 0, 100)

	doDelete := func() error {
		objects := make([]*s3.ObjectIdentifier, len(buffer))

		for i, v := range buffer {
			objects[i] = &s3.ObjectIdentifier{
				Key: aws.String(bucket.Prefix + v),
			}
		}

		req := s3.DeleteObjectsInput{
			Bucket: &bucket.Name,
			Delete: &s3.Delete{
				Objects: objects,
			},
		}

		if _, err := bucket.Client.DeleteObjects(&req); err != nil {
			return err
		}

		return nil
	}

	for object := range objects {
		buffer = append(buffer, object.Key)

		log.Printf("DELETE %s", object.Key)

		if len(buffer) == cap(buffer) {
			if err := doDelete(); err != nil {
				return err
			}

			buffer = buffer[0:0]
		}
	}

	if len(buffer) > 0 {
		if err := doDelete(); err != nil {
			return err
		}
	}

	return nil
}

func compareObjects(sourceObjects, destObjects <-chan *Object, toCopy, toDelete chan<- *Object) {
	sourceFinished := false
	destFinished := false

	var sourceCurrent *Object
	var destCurrent *Object

	buffer := func(from <-chan *Object, to **Object, finished *bool) {
		if *finished || *to != nil {
			return
		}

		obj, ok := <-from

		*finished = !ok

		if ok {
			*to = obj
		}
	}

	type Action int

	const (
		Copy Action = iota
		Skip
		Delete
	)

	cmp := func(a, b *Object) Action {
		if a == nil && b == nil {
			return Skip
		}

		if a == nil {
			return Delete
		}

		if b == nil {
			return Copy
		}

		if a.Key == b.Key {
			return Skip
		}

		if a.Key > b.Key {
			return Delete
		}

		return Copy
	}

	for {
		buffer(sourceObjects, &sourceCurrent, &sourceFinished)
		buffer(destObjects, &destCurrent, &destFinished)

		if sourceFinished && destFinished {
			close(toCopy)
			close(toDelete)
			return
		}

		action := cmp(sourceCurrent, destCurrent)

		switch action {
		case Copy:
			toCopy <- sourceCurrent
			sourceCurrent = nil
		case Skip:
			if *overwrite {
				toCopy <- sourceCurrent
			} else {
				log.Printf("SKIP   %s", sourceCurrent.Key)
			}

			sourceCurrent = nil
			destCurrent = nil
		case Delete:
			toDelete <- destCurrent
			destCurrent = nil
		}
	}
}

func runCopy(sourceBucket, destBucket *Bucket) error {
	const objectChannelBufferSize = 1024

	sourceObjects := make(chan *Object, objectChannelBufferSize)
	destObjects := make(chan *Object, objectChannelBufferSize)
	objectsToCopy := make(chan *Object, objectChannelBufferSize)
	objectsToDelete := make(chan *Object, objectChannelBufferSize)
	listSourceObjectsError := make(chan error, 1)
	listDestObjectsError := make(chan error, 1)
	copyError := make(chan error, 1)
	deleteError := make(chan error, 1)

	go func() {
		listSourceObjectsError <- listBucket(sourceBucket, sourceObjects)
	}()

	go func() {
		listDestObjectsError <- listBucket(destBucket, destObjects)
	}()

	go func() {
		copyError <- copyObjects(sourceBucket, destBucket, objectsToCopy)
	}()

	go func() {
		deleteError <- deleteObjects(destBucket, objectsToDelete)
	}()

	compareObjects(sourceObjects, destObjects, objectsToCopy, objectsToDelete)

	for {
		if listSourceObjectsError == nil && listDestObjectsError == nil && copyError == nil && deleteError == nil {
			return nil
		}

		select {
		case err := <-listSourceObjectsError:
			if err != nil {
				return fmt.Errorf("Error while listing keys in source bucket: %s", err)
			}

			listSourceObjectsError = nil
		case err := <-listDestObjectsError:
			if err != nil {
				return fmt.Errorf("Error while listing keys in destination bucket: %s", err)
			}

			listDestObjectsError = nil
		case err := <-copyError:
			if err != nil {
				return fmt.Errorf("Error while copying keys to destination bucket: %s", err)
			}

			copyError = nil
		case err := <-deleteError:
			if err != nil {
				return fmt.Errorf("Error while deleting keys from destination bucket: %s", err)
			}

			deleteError = nil
		}
	}
}

var sourceRegion = flag.String("sourceRegion", "", "Region of the source S3 bucket (auto detected if not specified)")
var destRegion = flag.String("destRegion", "", "Region of the destination S3 bucket (auto detected if not specified)")
var dryRun = flag.Bool("dryRun", false, "Don't actually do the copy")
var overwrite = flag.Bool("overwrite", false, "Force copy even if destination key already exists")
var opTimeout = flag.Duration("opTimeout", 3*time.Minute, "Timeout for S3 operations")

func main() {
	flag.Usage = func() {
		const usage = `Usage: %s SOURCE DEST

Mirrors a S3 folder to another one, potentially in a different bucket.

SOURCE and DEST should be S3 URLs in the form s3://bucket-name/prefix .
Objects metadata is preserved during the copy.

Additional command line options:
`

		fmt.Fprintf(os.Stderr, usage, os.Args[0])
		flag.PrintDefaults()
	}

	flag.Parse()

	if flag.NArg() != 2 {
		flag.Usage()
		os.Exit(1)
	}

	// We only use the default client to fetch bucket location.
	defaults.DefaultConfig.Region = aws.String("us-east-1")
	defaults.DefaultConfig.S3ForcePathStyle = aws.Bool(true)
	defaults.DefaultConfig.MaxRetries = aws.Int(10)
	http.DefaultClient.Timeout = *opTimeout

	sourceUrl := flag.Arg(0)
	destUrl := flag.Arg(1)

	sourceBucket, err := NewBucket(sourceUrl, *sourceRegion)

	if err != nil {
		log.Fatalf("Error while configuring source bucket: %s", err)
	}

	destBucket, err := NewBucket(destUrl, *destRegion)

	if err != nil {
		log.Fatalf("Error while configuring destination bucket: %s", err)
	}

	if err := runCopy(sourceBucket, destBucket); err != nil {
		log.Fatalf(err.Error())
	}
}
