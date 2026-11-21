// Command s3cleanup removes expired objects from an S3-compatible bucket in batches.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func main() {
	endpoint := flag.String("endpoint", "127.0.0.1:9005", "S3 endpoint")
	access := flag.String("access", "minioadmin", "access key")
	secret := flag.String("secret", "minioadmin", "secret key")
	bucket := flag.String("bucket", "zim", "bucket")
	prefix := flag.String("prefix", "", "object prefix")
	age := flag.Duration("age", 720*time.Hour, "delete objects older than this")
	batch := flag.Int("batch", 500, "delete batch size")
	dryRun := flag.Bool("dry-run", false, "list only")
	flag.Parse()

	ctx := context.Background()
	client, err := minio.New(*endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(*access, *secret, ""),
		Secure: false,
		Region: "us-east-1",
	})
	if err != nil {
		fail("client: %v", err)
	}
	cutoff := time.Now().Add(-*age)
	objects := client.ListObjects(ctx, *bucket, minio.ListObjectsOptions{Prefix: *prefix, Recursive: true})
	pending := make([]minio.ObjectInfo, 0, *batch)
	removed, failed := 0, 0
	flush := func() {
		if len(pending) == 0 {
			return
		}
		if *dryRun {
			fmt.Printf("dry-run objects=%d first=%s\n", len(pending), pending[0].Key)
			pending = pending[:0]
			return
		}
		ch := make(chan minio.ObjectInfo, len(pending))
		for _, object := range pending {
			ch <- object
		}
		close(ch)
		batchFailed := 0
		for result := range client.RemoveObjects(ctx, *bucket, ch, minio.RemoveObjectsOptions{}) {
			if result.Err != nil {
				batchFailed++
				fmt.Fprintf(os.Stderr, "remove %s: %v\n", result.ObjectName, result.Err)
			}
		}
		removed += len(pending) - batchFailed
		failed += batchFailed
		pending = pending[:0]
	}
	for object := range objects {
		if object.Err != nil {
			fail("list: %v", object.Err)
		}
		if object.LastModified.After(cutoff) {
			continue
		}
		pending = append(pending, object)
		if len(pending) == *batch {
			flush()
		}
	}
	flush()
	fmt.Printf("removed=%d failed=%d dry_run=%v\n", removed, failed, *dryRun)
	if failed > 0 {
		os.Exit(1)
	}
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
