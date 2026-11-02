FROM golang:1.24-alpine AS build
RUN apk add --no-cache ca-certificates git
ENV CGO_ENABLED=0 GOMAXPROCS=2 GOPROXY=https://proxy.golang.org,direct
# 固定为 2026-10-14 实际构建并经完整 DAG 验证的版本；该版本串取自镜像内
# /licenses/build-info.txt（mod github.com/minio/minio v0.0.0-20260212201848-7aac2a2c5b7c）。
# 改为 latest 会在下次构建时静默漂移，勿改回。重新构建需要可用的 Go 代理。
ARG MINIO_VERSION=v0.0.0-20260212201848-7aac2a2c5b7c
RUN go install -p 2 -ldflags="-s -w" github.com/minio/minio@${MINIO_VERSION}
RUN go version -m /go/bin/minio > /go/bin/build-info.txt \
    && cp /go/pkg/mod/github.com/minio/minio@*/LICENSE /go/bin/LICENSE

FROM alpine:3.22
RUN apk add --no-cache ca-certificates
COPY --from=build /go/bin/minio /usr/local/bin/minio
COPY --from=build /go/bin/build-info.txt /licenses/build-info.txt
COPY --from=build /go/bin/LICENSE /licenses/LICENSE
LABEL org.opencontainers.image.source="https://github.com/minio/minio" \
      org.opencontainers.image.licenses="AGPL-3.0" \
      org.opencontainers.image.description="Community MinIO built locally for the tutorial deployment"
EXPOSE 9000 9001
ENTRYPOINT ["minio"]
