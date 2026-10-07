# syntax=docker/dockerfile:1
FROM --platform=$BUILDPLATFORM node:26.10.0-alpine@sha256:0b36e8c136b94cd4fcf02188228e76c31ad5872eef3fec8cbd2eee500cfd9e80 AS assets
WORKDIR /src
COPY package.json package-lock.json ./
RUN npm ci --ignore-scripts --no-audit --no-fund && npm run assets

FROM --platform=$BUILDPLATFORM golang:1.27.1-alpine@sha256:8a5910f31396cd4d89662f56c68b3ae31d374308270a1c3bd96672ee5ed43414 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY VERSION version.go ./
COPY cmd ./cmd
COPY internal ./internal
COPY --from=assets /src/internal/console/static/vendor ./internal/console/static/vendor
ARG TARGETOS TARGETARCH
ARG VERSION
ARG REVISION
ARG MODIFIED
RUN test -z "$VERSION" || test "$VERSION" = "$(cat VERSION)"
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath \
    -ldflags="-s -w -X github.com/PadenZach/maestro/internal/console.Revision=${REVISION} -X github.com/PadenZach/maestro/internal/console.Modified=${MODIFIED}" \
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
ENV MAESTRO_LISTEN_ADDR=:8090
EXPOSE 8090
ENTRYPOINT ["/maestro"]
