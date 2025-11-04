package main

import (
	"testing"
)

func TestParseImageReference(t *testing.T) {
	tests := []struct {
		name       string
		imageRef   string
		wantHost   string
		wantImage  string
		wantRef    string
		wantDigest bool
		wantErr    bool
	}{
		{
			name:       "docker hub library image with tag",
			imageRef:   "docker.io/library/nginx:latest",
			wantHost:   "docker.io",
			wantImage:  "library/nginx",
			wantRef:    "latest",
			wantDigest: false,
		},
		{
			name:       "docker hub short form",
			imageRef:   "nginx:latest",
			wantHost:   "docker.io",
			wantImage:  "nginx",
			wantRef:    "latest",
			wantDigest: false,
		},
		{
			name:       "docker hub user image",
			imageRef:   "docker.io/myuser/myimage:v1.0",
			wantHost:   "docker.io",
			wantImage:  "myuser/myimage",
			wantRef:    "v1.0",
			wantDigest: false,
		},
		{
			name:       "ghcr with digest",
			imageRef:   "ghcr.io/owner/repo@sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
			wantHost:   "ghcr.io",
			wantImage:  "owner/repo",
			wantRef:    "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
			wantDigest: true,
		},
		{
			name:       "gcr image",
			imageRef:   "gcr.io/project/image:tag",
			wantHost:   "gcr.io",
			wantImage:  "project/image",
			wantRef:    "tag",
			wantDigest: false,
		},
		{
			name:       "quay.io image",
			imageRef:   "quay.io/organization/repository:latest",
			wantHost:   "quay.io",
			wantImage:  "organization/repository",
			wantRef:    "latest",
			wantDigest: false,
		},
		{
			name:       "image without tag defaults to latest",
			imageRef:   "nginx",
			wantHost:   "docker.io",
			wantImage:  "nginx",
			wantRef:    "latest",
			wantDigest: false,
		},
		{
			name:       "multi-level image path",
			imageRef:   "gcr.io/project/team/app:v1.0.0",
			wantHost:   "gcr.io",
			wantImage:  "project/team/app",
			wantRef:    "v1.0.0",
			wantDigest: false,
		},
		{
			name:       "localhost registry",
			imageRef:   "localhost:5000/myimage:latest",
			wantHost:   "localhost:5000",
			wantImage:  "myimage",
			wantRef:    "latest",
			wantDigest: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotHost, gotImage, gotRef, gotDigest, err := parseImageReference(tt.imageRef)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseImageReference() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err != nil {
				return
			}

			if gotHost != tt.wantHost {
				t.Errorf("host = %v, want %v", gotHost, tt.wantHost)
			}
			if gotImage != tt.wantImage {
				t.Errorf("image = %v, want %v", gotImage, tt.wantImage)
			}
			if gotRef != tt.wantRef {
				t.Errorf("reference = %v, want %v", gotRef, tt.wantRef)
			}
			if gotDigest != tt.wantDigest {
				t.Errorf("isDigest = %v, want %v", gotDigest, tt.wantDigest)
			}
		})
	}
}
