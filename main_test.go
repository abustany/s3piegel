package main

import (
	"bytes"
	"fmt"
	"github.com/AdRoll/goamz/s3/s3test"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/service/s3"
	"reflect"
	"strings"
	"testing"
)

var testIdx = 0

func runTest(t *testing.T, testfunc func(t *testing.T, src, dst *Bucket)) {
	server, err := s3test.NewServer(nil)

	if err != nil {
		t.Fatalf("Cannot start test S3 server: %s", err)
	}

	defer server.Quit()

	config := aws.NewConfig()
	config.Credentials = credentials.NewStaticCredentials("xx", "yy", "zz")
	config.DisableSSL = aws.Bool(false)
	config.Endpoint = aws.String(server.URL())
	config.Region = aws.String("us-east-1")
	config.S3ForcePathStyle = aws.Bool(true)

	srcBucketName := fmt.Sprintf("test-%d-src", testIdx)
	dstBucketName := fmt.Sprintf("test-%d-dst", testIdx)
	testIdx++

	s3client := s3.New(config)

	for _, bucketName := range []string{srcBucketName, dstBucketName} {
		req := s3.CreateBucketInput{
			Bucket: aws.String(bucketName),
			CreateBucketConfiguration: &s3.CreateBucketConfiguration{
				LocationConstraint: aws.String("us-east-1"),
			},
		}

		_, err := s3client.CreateBucket(&req)

		if err != nil {
			t.Fatalf("Error while creating bucket %s: %s", bucketName, err)
		}
	}

	srcBucket := Bucket{
		Name:   srcBucketName,
		Client: s3client,
	}

	dstBucket := Bucket{
		Name:   dstBucketName,
		Client: s3client,
	}

	testfunc(t, &srcBucket, &dstBucket)
}

type keyMeta struct {
	key  string
	meta map[string]string
}

func createKeys(t *testing.T, bucket *Bucket, keys ...keyMeta) []keyMeta {
	for _, key := range keys {
		req := s3.PutObjectInput{
			Body:   bytes.NewReader([]byte("Data")),
			Bucket: aws.String(bucket.Name),
			Key:    aws.String(key.key),
		}

		if key.meta != nil {
			req.Metadata = map[string]*string{}

			for k, v := range key.meta {
				req.Metadata[k] = &v
			}
		}

		if _, err := bucket.Client.PutObject(&req); err != nil {
			t.Fatalf("Error while creating key %s in %s: %s", key, bucket.Name, err)
		}
	}

	return keys
}

func keyList(keys []keyMeta) []string {
	strs := make([]string, len(keys))

	for i, key := range keys {
		strs[i] = key.key
	}

	return strs
}

func ensureKeys(t *testing.T, bucket *Bucket, keys ...keyMeta) {
	allKeys := []keyMeta{}
	listRequest := s3.ListObjectsInput{
		Bucket: aws.String(bucket.Name),
		Prefix: aws.String(bucket.Prefix),
	}

	callback := func(result *s3.ListObjectsOutput, lastPage bool) bool {
		for _, obj := range result.Contents {
			key := keyMeta{
				key: *obj.Key,
			}

			req := s3.HeadObjectInput{
				Bucket: aws.String(bucket.Name),
				Key:    aws.String(*obj.Key),
			}

			res, err := bucket.Client.HeadObject(&req)

			if err != nil {
				t.Fatalf("Error while retrieving metadata for key %s: %s", *obj.Key, err)
			}

			if len(res.Metadata) != 0 {
				key.meta = map[string]string{}

				for k, v := range res.Metadata {
					key.meta[k] = *v
				}
			}

			allKeys = append(allKeys, key)
		}

		return true
	}

	if err := bucket.Client.ListObjectsPages(&listRequest, callback); err != nil {
		t.Fatalf("Error while listing keys: %s", err)
	}

	if len(keys) != len(allKeys) {
		t.Fatalf("Expected keys [%s] do not match actual keys [%s]", keyList(keys), keyList(allKeys))
	}

	for i := range keys {
		if !reflect.DeepEqual(keys[i], allKeys[i]) {
			t.Fatalf("Keys do not match at index %d: expected %s, got %s", i, keys[i], allKeys[i])
		}
	}
}

func doCopy(t *testing.T, srcBucket, dstBucket *Bucket) {
	if err := runCopy(srcBucket, dstBucket); err != nil {
		t.Fatalf("Error while running copy: %s", err)
	}
}

// double slash elimitation
// missing / updated / deleted
func TestCopySourceToEmpty(t *testing.T) {
	runTest(t, testCopySourceToEmpty)
}

func testCopySourceToEmpty(t *testing.T, srcBucket, dstBucket *Bucket) {
	keys := []keyMeta{
		{"prefix/a", map[string]string{"Content-Type": "application/json"}},
		{"prefix/b", nil},
		{"prefix/c", nil},
	}
	createKeys(t, srcBucket, keys...)
	srcBucket.Prefix = "prefix"
	dstBucket.Prefix = "dst"
	doCopy(t, srcBucket, dstBucket)

	expectedKeys := []keyMeta{
		{"dst/a", map[string]string{"Content-Type": "application/json"}},
		{"dst/b", nil},
		{"dst/c", nil},
	}
	ensureKeys(t, dstBucket, expectedKeys...)
}

func TestCopyUpdate(t *testing.T) {
	runTest(t, testCopyUpdate)
}

func testCopyUpdate(t *testing.T, srcBucket, dstBucket *Bucket) {
	keys := []keyMeta{
		{"prefix/a", map[string]string{"Content-Type": "application/json"}},
		{"prefix/b", nil},
		{"prefix/c", nil},
	}
	existingKeys := []keyMeta{
		{"prefix/b", nil},
		{"prefix/d", nil},
		{"other/z", nil},
	}
	createKeys(t, srcBucket, keys...)
	createKeys(t, dstBucket, existingKeys...)
	srcBucket.Prefix = "prefix"
	dstBucket.Prefix = "dst"
	doCopy(t, srcBucket, dstBucket)

	expectedKeys := []keyMeta{
		{"dst/a", map[string]string{"Content-Type": "application/json"}},
		{"dst/b", nil},
		{"dst/c", nil},
	}
	ensureKeys(t, dstBucket, expectedKeys...)

	// Check that nothing was touched outside the prefix
	req := s3.HeadObjectInput{
		Bucket: aws.String(dstBucket.Name),
		Key:    aws.String("other/z"),
	}

	_, err := dstBucket.Client.HeadObject(&req)

	if err != nil {
		t.Fatalf("Error while doing HEAD request")
	}
}

func TestCoalesceDoubleSlash(t *testing.T) {
	runTest(t, testCoalesceDoubleSlash)
}

func testCoalesceDoubleSlash(t *testing.T, srcBucket, dstBucket *Bucket) {
	keys := []keyMeta{
		{"prefix/a", nil},
	}
	createKeys(t, srcBucket, keys...)
	srcBucket.Prefix = "prefix/"
	dstBucket.Prefix = "dst/"
	doCopy(t, srcBucket, dstBucket)

	expectedKeys := []keyMeta{
		{"dst/a", nil},
	}
	ensureKeys(t, dstBucket, expectedKeys...)
}

func TestCompareKeys(t *testing.T) {
	testData := []struct {
		Name       string
		SourceKeys []string
		DestKeys   []string
		ToCopy     []string
		ToDelete   []string
	}{
		{
			"Empty destination",
			[]string{"a", "b", "c"},
			[]string{},
			[]string{"a", "b", "c"},
			[]string{},
		},
		{
			"Empty source",
			[]string{},
			[]string{"a", "b", "c"},
			[]string{},
			[]string{"a", "b", "c"},
		},
		{
			"Same in source and destination",
			[]string{"a", "b", "c"},
			[]string{"a", "b", "c"},
			[]string{},
			[]string{},
		},
		{
			"Some existing files in destination",
			[]string{"a", "b", "c"},
			[]string{"a", "b"},
			[]string{"c"},
			[]string{},
		},
		{
			"Some files removed in source",
			[]string{"a", "c"},
			[]string{"a", "b", "c"},
			[]string{},
			[]string{"b"},
		},
		{
			"Files added and removed",
			[]string{"a", "b", "c"},
			[]string{"a", "c", "d"},
			[]string{"b"},
			[]string{"d"},
		},
	}

	send := func(data []string) <-chan *Object {
		ch := make(chan *Object)

		go func() {
			for _, d := range data {
				ch <- &Object{
					Key: d,
				}
			}

			close(ch)
		}()

		return ch
	}

	receive := func(out chan<- []string) chan<- *Object {
		ch := make(chan *Object)

		go func() {
			all := []string{}

			for s := range ch {
				all = append(all, s.Key)
			}

			out <- all
			close(out)
		}()

		return ch
	}

	for _, d := range testData {
		toCopy := make(chan []string, 1)
		toDelete := make(chan []string, 1)

		compareObjects(send(d.SourceKeys), send(d.DestKeys), receive(toCopy), receive(toDelete))

		if keys := <-toCopy; !reflect.DeepEqual(d.ToCopy, keys) {
			t.Errorf("Keys to copy don't match for test case %s: expected %s, got %s", d.Name, strings.Join(d.ToCopy, ", "), strings.Join(keys, ", "))
		}

		if keys := <-toDelete; !reflect.DeepEqual(d.ToDelete, keys) {
			t.Errorf("Keys to delete don't match for test case %s: expected %s, got %s", d.Name, strings.Join(d.ToDelete, ", "), strings.Join(keys, ", "))
		}
	}
}
