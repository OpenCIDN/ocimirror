package test_test

import (
	"log"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"time"

	"github.com/OpenCIDN/ocimirror/pkg/cache"
	"github.com/OpenCIDN/ocimirror/pkg/gateway"
	"github.com/OpenCIDN/ocimirror/pkg/manifests"
	"github.com/OpenCIDN/ocimirror/pkg/transport"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/wzshiming/sss"
)

func OnlyForward() (*httptest.Server, string, func()) {
	transportOpts := []transport.Option{
		transport.WithUserAndPass(nil),
	}

	tp, err := transport.NewTransport(transportOpts...)
	if err != nil {
		log.Fatal(err)
	}

	tp = transport.NewLogTransport(tp, slog.Default(), 0)

	httpClient := &http.Client{
		Transport: tp,
	}

	gw, err := gateway.NewGateway(
		gateway.WithClient(httpClient),
	)
	if err != nil {
		log.Fatal(err)
	}

	mux := http.NewServeMux()

	mux.Handle("/v2/", gw)

	return httptest.NewServer(mux), "only proxy", func() {}
}

func WithCache() (*httptest.Server, string, func()) {
	bucket := "sss-test-bucket"

	url := `sss://minioadmin:minioadmin@` + bucket + `.region?forcepathstyle=true&secure=false&regionendpoint=http://127.0.0.1:9000`

	s, err := sss.NewSSS(sss.WithURL(url))
	if err != nil {
		log.Fatal(err)
	}

	err = exec.Command("docker", "compose", "up", "-d").Run()
	if err != nil {
		log.Fatal(err)
	}

	time.Sleep(2 * time.Second)

	_, err = s.S3().HeadBucket(&s3.HeadBucketInput{
		Bucket: aws.String(bucket),
	})

	if err != nil {
		if s3Err, ok := err.(awserr.Error); ok && s3Err.Code() == "NotFound" {
			_, err = s.S3().CreateBucket(&s3.CreateBucketInput{
				Bucket: aws.String(bucket),
			})
			if err != nil {
				log.Fatal(err)
			}
		} else {
			log.Fatal(err)
		}

		s.S3().CreateBucket(&s3.CreateBucketInput{
			Bucket: aws.String(bucket),
		})
	}

	transportOpts := []transport.Option{
		transport.WithUserAndPass(nil),
	}

	tp, err := transport.NewTransport(transportOpts...)
	if err != nil {
		log.Fatal(err)
	}

	tp = transport.NewLogTransport(tp, slog.Default(), 0)

	httpClient := &http.Client{
		Transport: tp,
	}

	c, err := cache.NewCache(cache.WithStorageDriver(s))
	if err != nil {
		log.Fatal(err)
	}

	manifest, err := manifests.NewManifests(
		manifests.WithClient(httpClient),
		manifests.WithCache(c),
	)
	if err != nil {
		log.Fatal(err)
	}

	gw, err := gateway.NewGateway(
		gateway.WithClient(httpClient),
		gateway.WithManifests(manifest),
	)
	if err != nil {
		log.Fatal(err)
	}

	mux := http.NewServeMux()

	mux.Handle("/v2/", gw)

	return httptest.NewServer(mux), "with cache", func() {
		err = exec.Command("docker", "compose", "down").Run()
		if err != nil {
			log.Fatal(err)
		}
	}
}
