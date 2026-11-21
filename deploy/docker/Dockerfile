# syntax=docker/dockerfile:1.7

# ---- 1. Dependencies ---------------------------------------------------------
FROM golang:1.27-alpine AS deps
WORKDIR /src
COPY go.mod go.sum ./
COPY zimfw/go.mod zimfw/go.sum ./zimfw/
RUN --mount=type=cache,target=/go/pkg/mod go mod download

# ---- 2. Build the 11 binaries in one layer ----------------------------------
FROM deps AS build
COPY . .
ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_TIME=unknown
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath \
      -ldflags "-s -w \
        -X github.com/zciyuan/ZIM/internal/version.Version=${VERSION} \
        -X github.com/zciyuan/ZIM/internal/version.Commit=${COMMIT} \
        -X github.com/zciyuan/ZIM/internal/version.BuildTime=${BUILD_TIME}" \
      -o /out/ ./cmd/... \
    && mkdir -p /out/run \
    && touch /out/run/.keep

# ---- 3. Runtime -------------------------------------------------------------
FROM gcr.io/distroless/static-debian12:nonroot

ARG VERSION=dev
ARG COMMIT=unknown
LABEL org.opencontainers.image.title="zim-server" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${COMMIT}" \
      org.opencontainers.image.source="https://github.com/zciyuan/ZIM"

COPY --from=build --chown=65532:65532 /out/ /app/
WORKDIR /app
# /app/run must stay writable: the Nacos registry client writes its naming cache
# and logs under ./run/nacos. Mount an emptyDir here if the root filesystem is
# mounted read-only.
# No ENTRYPOINT: Kubernetes selects the binary via command, e.g.
# command: ["/app/zim-gate"]
