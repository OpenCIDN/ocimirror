# ocimirror

**ocimirror** is an [OCI (Open Container Initiative)](https://opencontainers.org/) image registry mirror and proxy, providing a transparent caching layer for container images.

## Features

- **Transparent Proxying**: Mirrors container images from any OCI-compliant registry
- **Caching**: Reduces bandwidth and improves pull performance by caching images locally
- **Multi-Registry Support**: Works with Docker Hub, GitHub Container Registry, and other OCI registries
- **Easy Integration**: Simply prefix your image references - no configuration changes needed
- **Proactive Synchronization**: Pre-cache images using the sync command for faster distribution

This project builds upon [CRProxy](https://github.com/DaoCloud/crproxy) with enhanced features and improvements.

## Commands

ocimirror provides several commands for different use cases:

- **gateway**: OCI registry proxy and mirror with transparent caching
- **agent**: Blob serving agent for distributed image layers
- **auth**: Authentication server for token-based access control
- **sync**: Proactively synchronize OCI images to CIDN resources for pre-caching

## Usage

To use ocimirror, add a prefix to your container image references

### Docker

Add your mirror prefix to the image reference:

``` bash
docker pull <mirror-host>/docker.io/library/busybox
```

For example, using `m.daocloud.io` as the mirror:

``` bash
docker pull m.daocloud.io/docker.io/library/busybox
```

### Kubernetes

Add your mirror prefix to image references in your manifests:

``` yaml
image: <mirror-host>/docker.io/library/busybox
```

For example, using `m.daocloud.io` as the mirror:

``` yaml
image: m.daocloud.io/docker.io/library/busybox
```

## Proactive Image Synchronization

The `sync` command allows you to proactively create CIDN resources for OCI images, enabling pre-caching and faster distribution.

### Prerequisites

- A running Kubernetes cluster with CIDN support
- Access to a kubeconfig file
- A configured storage backend (e.g., S3, local storage)

### Basic Usage

Synchronize a single image:

``` bash
sync --images docker.io/library/busybox:latest \
     --storage-url s3://my-bucket/cache \
     --kubeconfig ~/.kube/config
```

Synchronize multiple images:

``` bash
sync --images docker.io/library/nginx:latest,docker.io/library/alpine:3.18 \
     --storage-url s3://my-bucket/cache \
     --kubeconfig ~/.kube/config
```

### Advanced Options

With authentication:

``` bash
sync --images ghcr.io/org/app:v1.0 \
     --storage-url s3://my-bucket/cache \
     --destination s3 \
     --kubeconfig ~/.kube/config \
     --user myuser:mypass@ghcr.io \
     --insecure-skip-tls-verify
```

Available flags:
- `--images`: OCI images to synchronize (required, comma-separated)
- `--storage-url`: Storage driver URL (required, e.g., s3://bucket/path)
- `--destination`: CIDN destination name (defaults to storage URL scheme)
- `--kubeconfig`: Path to kubeconfig file
- `--master`: Kubernetes API server address
- `-u, --user`: Registry credentials (format: user:pwd@host)
- `--insecure-skip-tls-verify`: Skip TLS certificate verification
- `--retry`: Number of retries for failed requests
- `--retry-interval`: Interval between retries
