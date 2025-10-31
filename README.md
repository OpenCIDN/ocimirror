# ocimirror

**ocimirror** is an [OCI (Open Container Initiative)](https://opencontainers.org/) image registry mirror and proxy, providing a transparent caching layer for container images.

## Features

- **Transparent Proxying**: Mirrors container images from any OCI-compliant registry
- **Caching**: Reduces bandwidth and improves pull performance by caching images locally
- **Multi-Registry Support**: Works with Docker Hub, GitHub Container Registry, and other OCI registries
- **Easy Integration**: Simply prefix your image references - no configuration changes needed

This project builds upon [CRProxy](https://github.com/DaoCloud/crproxy) with enhanced features and improvements.

## Installation

### Binary

Download the latest release from the [releases page](https://github.com/OpenCIDN/ocimirror/releases).

### Building from Source

```bash
go build -o ocimirror ./cmd/ocimirror
```

## Components

ocimirror provides three main components accessible via subcommands:

- **gateway** - Main OCI registry proxy and cache (default port: 18001)
- **agent** - Dedicated blob storage service (default port: 18002)  
- **auth** - Authentication and authorization service (default port: 18000)

### Running the Gateway

```bash
ocimirror gateway --address :8080
```

### Running the Agent (Blob Storage)

```bash
ocimirror agent --address :8081 --storage-url s3://bucket
```

### Running the Auth Service

```bash
ocimirror auth --address :8082
```

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

## Migration from Standalone Binaries

If you were using the standalone `gateway`, `agent`, or `auth` binaries, you can now use the unified `ocimirror` binary with subcommands:

- `gateway` → `ocimirror gateway`
- `agent` → `ocimirror agent`  
- `auth` → `ocimirror auth`

All command-line flags remain the same. The standalone binaries are deprecated and will be removed in a future release.

