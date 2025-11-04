# ocimirror

**ocimirror** is an [OCI (Open Container Initiative)](https://opencontainers.org/) image registry mirror and proxy, providing a transparent caching layer for container images.

## Features

- **Transparent Proxying**: Mirrors container images from any OCI-compliant registry
- **Caching**: Reduces bandwidth and improves pull performance by caching images locally
- **Multi-Registry Support**: Works with Docker Hub, GitHub Container Registry, and other OCI registries
- **Easy Integration**: Simply prefix your image references - no configuration changes needed

This project builds upon [CRProxy](https://github.com/DaoCloud/crproxy) with enhanced features and improvements.

## Usage

### Mirror Mode

To use ocimirror, add a prefix to your container image references

#### Docker

Add your mirror prefix to the image reference:

``` bash
docker pull <mirror-host>/docker.io/library/busybox
```

For example, using `m.daocloud.io` as the mirror:

``` bash
docker pull m.daocloud.io/docker.io/library/busybox
```

#### Kubernetes

Add your mirror prefix to image references in your manifests:

``` yaml
image: <mirror-host>/docker.io/library/busybox
```

For example, using `m.daocloud.io` as the mirror:

``` yaml
image: m.daocloud.io/docker.io/library/busybox
```

### Active Synchronization

Proactively synchronize OCI images using CIDN resources:

``` bash
# Sync a specific image tag
./sync --storage-url s3://mybucket --kubeconfig ~/.kube/config docker.io/library/nginx:latest

# Sync a specific image digest
./sync --storage-url s3://mybucket --kubeconfig ~/.kube/config docker.io/library/nginx@sha256:abc123...

# Sync multiple images
./sync --storage-url s3://mybucket --kubeconfig ~/.kube/config \
  ghcr.io/owner/repo:v1.0.0 \
  quay.io/org/image:latest

# Sync a specific platform (e.g., for multi-arch images)
./sync --storage-url s3://mybucket --kubeconfig ~/.kube/config \
  --platform linux/amd64 \
  docker.io/library/nginx:latest

# Sync ARM64 platform
./sync --storage-url s3://mybucket --kubeconfig ~/.kube/config \
  --platform linux/arm64 \
  ghcr.io/owner/repo:v1.0.0

# Sync ARM v7 platform with variant
./sync --storage-url s3://mybucket --kubeconfig ~/.kube/config \
  --platform linux/arm/v7 \
  docker.io/library/alpine:latest

# Sync multiple platforms
./sync --storage-url s3://mybucket --kubeconfig ~/.kube/config \
  --platform linux/amd64 \
  --platform linux/arm64 \
  --platform linux/arm/v7 \
  docker.io/library/nginx:latest
```

The sync command creates CIDN resources to proactively synchronize images to your cache, eliminating the need to wait for the first pull request. When platforms are specified, the command will parse manifest lists and sync the platform-specific manifests along with their config and layer blobs. Platform variants (like arm/v7, arm/v8) are supported for ARM architectures.
