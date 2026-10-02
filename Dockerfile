# syntax=docker/dockerfile:1
FROM --platform=$BUILDPLATFORM golang:1.27.1-alpine@sha256:8a5910f31396cd4d89662f56c68b3ae31d374308270a1c3bd96672ee5ed43414 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY VERSION version.go ./
COPY cmd ./cmd
COPY internal ./internal
ARG TARGETOS TARGETARCH
ARG VERSION
ARG REVISION
ARG MODIFIED
RUN test -z "$VERSION" || test "$VERSION" = "$(cat VERSION)"
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath \
    -ldflags="-s -w -X github.com/zpaden/maestro/internal/web.Revision=${REVISION} -X github.com/zpaden/maestro/internal/web.Modified=${MODIFIED}" \
    -o /out/maestro ./cmd/maestro

FROM scratch
ARG VERSION
ARG REVISION
ARG CREATED
ARG SOURCE=https://github.com/PadenZach/maestro
LABEL org.opencontainers.image.title="maestro" \
      org.opencontainers.image.description="DBOS workflow inspection service with an HTMX Console" \
      org.opencontainers.image.source=$SOURCE \
      org.opencontainers.image.url=$SOURCE \
      org.opencontainers.image.documentation="${SOURCE}#readme" \
      org.opencontainers.image.version=$VERSION \
      org.opencontainers.image.revision=$REVISION \
      org.opencontainers.image.created=$CREATED \
      org.opencontainers.image.licenses="MIT"
COPY --from=build /out/maestro /maestro
USER 65532:65532
# Container traffic arrives through its network interface, not its loopback.
ENV CONDUCTOR_LISTEN_ADDR=:8090 CONDUCTOR_ALLOW_REMOTE=true
EXPOSE 8090
ENTRYPOINT ["/maestro"]
