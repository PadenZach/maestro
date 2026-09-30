# syntax=docker/dockerfile:1
FROM --platform=$BUILDPLATFORM golang:1.26.8-alpine@sha256:8ac98ca534ac3f51e1f420a1dd2c15e74c75cfa0f23f3ad27eb5d7236c349a0c AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY cmd ./cmd
COPY internal ./internal
COPY docs/reference ./docs/reference
ARG TARGETOS TARGETARCH
ARG VERSION=dev
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath \
    -ldflags="-s -w -X github.com/zpaden/maestro/internal/web.Version=${VERSION}" \
    -o /out/maestro ./cmd/maestro

FROM scratch
ARG VERSION=dev
ARG REVISION
ARG CREATED
ARG SOURCE=https://github.com/zpaden/maestro
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
EXPOSE 8090
ENTRYPOINT ["/maestro"]
