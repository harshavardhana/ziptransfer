// Copyright 2023 Harshavardhana
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// Workers provides a bounded semaphore with the ability to wait until all
// concurrent jobs finish.
type Workers struct {
	wg    sync.WaitGroup
	queue chan struct{}
}

// New creates a Workers object which allows up to n jobs to proceed
// concurrently. n must be > 0.
func New(n int) (*Workers, error) {
	if n <= 0 {
		return nil, errors.New("n must be > 0")
	}

	queue := make(chan struct{}, n)
	for i := 0; i < n; i++ {
		queue <- struct{}{}
	}
	return &Workers{
		queue: queue,
	}, nil
}

// Take is how a job (goroutine) can Take its turn.
func (jt *Workers) Take() {
	jt.wg.Add(1)
	<-jt.queue
}

// Give is how a job (goroutine) can give back its turn once done.
func (jt *Workers) Give() {
	jt.queue <- struct{}{}
	jt.wg.Done()
}

// Wait waits for all ongoing concurrent jobs to complete
func (jt *Workers) Wait() {
	jt.wg.Wait()
}

func writeAsZip(srcClnt, destClnt *minio.Client, srcBucket, destBucket string, workers *Workers, entries []minio.ObjectInfo) {
	input := make(chan minio.SnowballObject, 1)
	opts := minio.SnowballOptions{
		Opts:     minio.PutObjectOptions{},
		InMemory: os.Getenv("INMEMORY") == "true",
		Compress: os.Getenv("COMPRESS") == "true",
		SkipErrs: os.Getenv("SKIPERRS") == "true",
	}

	t := time.Now()

	var total int64
	for _, entry := range entries {
		total += entry.Size
	}

	go func() {
		defer close(input)

		for _, entry := range entries {
			entry := entry

			workers.Take()
			go func() {
				defer workers.Give()
				r, err := srcClnt.GetObject(context.Background(), srcBucket,
					entry.Key, minio.GetObjectOptions{})
				if err != nil {
					fmt.Println("ERROR: ", err, entry.Key)
					return
				}

				input <- minio.SnowballObject{
					// Create path to store objects within the bucket.
					Key:     entry.Key,
					Size:    entry.Size,
					ModTime: entry.LastModified,
					Content: r,
					Close: func() {
						r.Close()
					},
				}
			}()
		}
		workers.Wait()
	}()

	// Collect and upload all entries.
	if err := destClnt.PutObjectsSnowball(context.Background(), destBucket, opts, input); err != nil {
		log.Fatalln(err)
	}

	fmt.Printf("Copied %d objects in %s successfully\n", len(entries), time.Since(t))
}

func main() {
	minio.MaxRetry = 1 // No retries

	var (
		srcPrefix  = os.Getenv("SRC_PREFIX")
		srcBucket  = os.Getenv("SRC_BUCKET")
		destBucket = os.Getenv("DEST_BUCKET")
	)

	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConnsPerHost:   1024,
		IdleConnTimeout:       time.Minute,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 10 * time.Second,
		// Set this value so that the underlying transport round-tripper
		// doesn't try to auto decode the body of objects with
		// content-encoding set to `gzip`.
		//
		// Refer:
		//    https://golang.org/src/net/http/transport.go?h=roundTrip#L1843
		DisableCompression: true,
		TLSClientConfig: &tls.Config{
			MinVersion:         tls.VersionTLS12,
			InsecureSkipVerify: true,
		},
	}
	srcClnt, err := minio.New(os.Getenv("SRC_SERVER"), &minio.Options{
		Creds:     credentials.NewStaticV4(os.Getenv("SRC_ACCESS_KEY"), os.Getenv("SRC_SECRET_KEY"), ""),
		Secure:    os.Getenv("SRC_SECURE") == "true",
		Transport: transport,
	})
	if err != nil {
		log.Fatal(err)
	}

	destClnt, err := minio.New(os.Getenv("DEST_SERVER"), &minio.Options{
		Creds:     credentials.NewStaticV4(os.Getenv("DEST_ACCESS_KEY"), os.Getenv("DEST_SECRET_KEY"), ""),
		Secure:    os.Getenv("DEST_SECURE") == "true",
		Transport: transport,
	})
	if err != nil {
		log.Fatal(err)
	}

	opts := minio.ListObjectsOptions{
		Recursive: true,
		Prefix:    srcPrefix,
	}

	workers, err := New(runtime.GOMAXPROCS(0))
	if err != nil {
		log.Fatal(err)
		return
	}

	objectsCh := srcClnt.ListObjects(context.Background(), srcBucket, opts)

	var entries []minio.ObjectInfo

	// List all objects from a bucket-name with a matching prefix.
	for {
		select {
		case object, ok := <-objectsCh:
			if !ok {
				writeAsZip(srcClnt, destClnt, srcBucket, destBucket, workers, entries)
				return
			}
			if object.Err != nil {
				fmt.Printf("ERROR: listing failed %v\n", object.Err)
				continue
			}
			entries = append(entries, object)
			if len(entries) == 100 {
				writeAsZip(srcClnt, destClnt, srcBucket, destBucket, workers, entries)
				entries = []minio.ObjectInfo{}
			}
		}
	}
}
