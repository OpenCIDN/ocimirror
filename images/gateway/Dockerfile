
FROM m.daocloud.io/docker.io/library/golang:1.24.7 AS builder

WORKDIR /workspace

COPY . .

RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    CGO_ENABLED=0 go build -o ocimirror ./cmd/ocimirror

FROM m.daocloud.io/docker.io/library/alpine:3.22
WORKDIR /
COPY --from=builder /workspace/ocimirror .

ENTRYPOINT ["/ocimirror", "gateway"]
