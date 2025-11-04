package main

import (
	"testing"

	"github.com/google/go-containerregistry/pkg/name"
)

func TestImageReferenceParsing(t *testing.T) {
	tests := []struct {
		name       string
		imageRef   string
		wantReg    string
		wantRepo   string
		wantTag    string
		wantDigest string
		wantErr    bool
	}{
		{
			name:     "docker hub library image with tag",
			imageRef: "docker.io/library/nginx:latest",
			wantReg:  "index.docker.io",
			wantRepo: "library/nginx",
			wantTag:  "latest",
		},
		{
			name:     "docker hub short form",
			imageRef: "nginx:latest",
			wantReg:  "index.docker.io",
			wantRepo: "library/nginx",
			wantTag:  "latest",
		},
		{
			name:     "docker hub user image",
			imageRef: "docker.io/myuser/myimage:v1.0",
			wantReg:  "index.docker.io",
			wantRepo: "myuser/myimage",
			wantTag:  "v1.0",
		},
		{
			name:       "ghcr with digest",
			imageRef:   "ghcr.io/owner/repo@sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
			wantReg:    "ghcr.io",
			wantRepo:   "owner/repo",
			wantDigest: "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
		},
		{
			name:     "gcr image",
			imageRef: "gcr.io/project/image:tag",
			wantReg:  "gcr.io",
			wantRepo: "project/image",
			wantTag:  "tag",
		},
		{
			name:     "quay.io image",
			imageRef: "quay.io/organization/repository:latest",
			wantReg:  "quay.io",
			wantRepo: "organization/repository",
			wantTag:  "latest",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ref, err := name.ParseReference(tt.imageRef, name.WeakValidation)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseReference() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err != nil {
				return
			}

			gotReg := ref.Context().RegistryStr()
			gotRepo := ref.Context().RepositoryStr()

			if gotReg != tt.wantReg {
				t.Errorf("registry = %v, want %v", gotReg, tt.wantReg)
			}
			if gotRepo != tt.wantRepo {
				t.Errorf("repository = %v, want %v", gotRepo, tt.wantRepo)
			}

			if tt.wantTag != "" {
				if tag, ok := ref.(name.Tag); ok {
					if tag.TagStr() != tt.wantTag {
						t.Errorf("tag = %v, want %v", tag.TagStr(), tt.wantTag)
					}
				} else {
					t.Errorf("expected tag reference, got %T", ref)
				}
			}

			if tt.wantDigest != "" {
				if digest, ok := ref.(name.Digest); ok {
					if digest.DigestStr() != tt.wantDigest {
						t.Errorf("digest = %v, want %v", digest.DigestStr(), tt.wantDigest)
					}
				} else {
					t.Errorf("expected digest reference, got %T", ref)
				}
			}
		})
	}
}
