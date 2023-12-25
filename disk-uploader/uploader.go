package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	xfilepath "github.com/minio/filepath"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/schollz/progressbar/v3"
)

// GOOS specific ignore list.
var ignoreFiles = map[string][]string{
	"darwin":  {"*.DS_Store"},
	"default": {"lost+found"},
}

// isIgnoredFile returns true if 'filename' is on the exclude list.
func isIgnoredFile(filename string) bool {
	matchFile := filepath.Base(filename)

	// OS specific ignore list.
	for _, ignoredFile := range ignoreFiles[runtime.GOOS] {
		matched, e := filepath.Match(ignoredFile, matchFile)
		if e != nil {
			panic(e)
		}
		if matched {
			return true
		}
	}

	// Default ignore list for all OSes.
	for _, ignoredFile := range ignoreFiles["default"] {
		matched, e := filepath.Match(ignoredFile, matchFile)
		if e != nil {
			panic(e)
		}
		if matched {
			return true
		}
	}

	return false
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	var (
		// Note: These constants are dummy values,
		// please replace them with values for your setup.
		YOURACCESSKEYID     = os.Getenv("ACCESS_KEY")
		YOURSECRETACCESSKEY = os.Getenv("SECRET_KEY")
		YOURENDPOINT        = os.Getenv("ENDPOINT")
		YOURBUCKET          = os.Getenv("BUCKET")
	)

	// Requests are always secure (HTTPS) by default. Set secure=false to enable insecure (HTTP) access.
	// This boolean value is the last argument for New().
	tr := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConnsPerHost:   1024,
		ResponseHeaderTimeout: time.Minute,
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
	}

	if os.Getenv("SECURE") == "on" {
		tr.TLSClientConfig = &tls.Config{
			// Can't use SSLv3 because of POODLE and BEAST
			// Can't use TLSv1.0 because of POODLE and BEAST using CBC cipher
			// Can't use TLSv1.1 because of RC4 cipher usage
			MinVersion:         tls.VersionTLS12,
			InsecureSkipVerify: true,
		}
	}

	// New returns an Amazon S3 compatible client object. API compatibility (v2 or v4) is automatically
	// determined based on the Endpoint value.
	minioClient, err := minio.New(YOURENDPOINT, &minio.Options{
		Creds:     credentials.NewStaticV4(YOURACCESSKEYID, YOURSECRETACCESSKEY, ""),
		Secure:    os.Getenv("SECURE") == "on",
		Transport: tr,
	})
	if err != nil {
		log.Fatalln(err)
	}

	input := make(chan minio.SnowballObject)
	opts := minio.SnowballOptions{
		Opts: minio.PutObjectOptions{
			DisableMultipart:     true,
			DisableContentSha256: true,
		},
		// Keep in memory. We use this since we have small total payload.
		InMemory: true,
		// Compress data when uploading to a MinIO host.
		Compress: true,
		// Skip any read errors.
		// SkipErrs: true,
	}

	// Generate
	go func() {
		defer close(input)

		walkFn := func(path string, fi os.FileInfo, err error) error {
			// If file path ends with filepath.Separator and equals to root path, skip it.
			if strings.HasSuffix(path, string(filepath.Separator)) {
				return nil
			}

			// We would never need to print system root path '/'.
			if path == "/" {
				return nil
			}

			// Ignore files from ignore list.
			if isIgnoredFile(fi.Name()) {
				return nil
			}

			if err != nil {
				if strings.Contains(err.Error(), "operation not permitted") {
					return nil
				}
				if os.IsPermission(err) {
					return nil
				}
				return err
			}

			if fi.Mode()&os.ModeSymlink == os.ModeSymlink {
				fi, err = os.Stat(path)
				if err != nil {
					// Ignore any errors for symlink
					return nil
				}
			}

			if fi.Mode().IsRegular() {
				input <- minio.SnowballObject{
					// Create path to store objects within the bucket.
					Key:     path,
					Size:    fi.Size(),
					ModTime: fi.ModTime(),
				}
			}

			return nil
		}

		root := os.Args[1]
		if err := xfilepath.Walk(root, walkFn); err != nil {
			log.Fatalf("%s: %v\n", root, err)
		}
	}()

	pb := progressbar.NewOptions(-1, progressbar.OptionSetWidth(10),
		progressbar.OptionSetPredictTime(true),
		progressbar.OptionShowBytes(true),
		progressbar.OptionUseIECUnits(true),
	)
	pb.Reset()

	doneCh := make(chan struct{})
	go func() {
		defer close(doneCh)

		var totalObjs int
		var totalSize int64
		var entries []minio.SnowballObject
		for {
			select {
			case sobj, ok := <-input:
				if !ok {
					if err := writeAsZip(minioClient, YOURBUCKET, opts, entries); err != nil {
						pb.Describe(fmt.Sprintf("uploading failed at the end: %v", err))
					} else {
						pb.Describe(fmt.Sprintf("finished uploading %d number of objects, total size of %s", totalObjs, humanize.IBytes(uint64(totalSize))))
					}
					return
				}

				entries = append(entries, sobj)
				if len(entries) == 100 {
					if err := writeAsZip(minioClient, YOURBUCKET, opts, entries); err != nil {
						pb.Describe(fmt.Sprintf("uploading failed: %v", err))
						return
					}
					entries = []minio.SnowballObject{}
				}
				totalObjs++
				totalSize += sobj.Size
				pb.Set64(totalSize)
			}
		}
	}()

	<-doneCh
	pb.Exit()
	fmt.Println()

	// Objects successfully uploaded.
}

func writeAsZip(clnt *minio.Client, bucket string, opts minio.SnowballOptions, entries []minio.SnowballObject) error {
	input := make(chan minio.SnowballObject, 1)

	go func() {
		defer close(input)

		for _, entry := range entries {
			f, err := os.Open(entry.Key)
			if err != nil {
				continue
			}
			entry.Content = f
			entry.Close = func() { f.Close() }
			input <- entry
		}
	}()

	// Collect and upload all entries.
	return clnt.PutObjectsSnowball(context.Background(), bucket, opts, input)
}
