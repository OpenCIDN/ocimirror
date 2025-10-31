# ocimirror

**ocimirror** is an OCI (Open Container Initiative) image registry mirror and proxy, providing a transparent caching layer for container images. It's part of the OpenCIDN (Open Container Image Delivery Network) project.

## Features

- **Transparent Proxying**: Mirrors container images from any OCI-compliant registry
- **Caching**: Reduces bandwidth and improves pull performance by caching images locally
- **Multi-Registry Support**: Works with Docker Hub, GitHub Container Registry, and other OCI registries
- **Easy Integration**: Simply prefix your image references - no configuration changes needed

This project builds upon [CRProxy](https://github.com/DaoCloud/crproxy) with enhanced features and improvements.

For deployment instructions, see the [documentation](https://github.com/OpenCIDN/docs).

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
