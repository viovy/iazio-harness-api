# syntax=docker/dockerfile:1

FROM debian:bookworm-slim AS runtime-base
RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates chromium \
  && rm -rf /var/lib/apt/lists/*

FROM golang:1.26-bookworm AS build
ARG TARGETOS=linux
ARG TARGETARCH=amd64
ARG GIT_VERSION=0.1.0-dev
ARG GIT_COMMIT=unknown
WORKDIR /src
COPY go.mod go.sum ./
COPY cmd cmd
COPY internal internal
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath \
  -ldflags="-s -w \
  -X github.com/viovy/iazio-harness-api/internal/version.Version=${GIT_VERSION} \
  -X github.com/viovy/iazio-harness-api/internal/version.Commit=${GIT_COMMIT}" \
  -o /out/iazio-harness-api ./cmd/iazio-harness-api

FROM runtime-base
RUN useradd --create-home --uid 10001 app
COPY --from=build /out/iazio-harness-api /usr/local/bin/iazio-harness-api
USER app
EXPOSE 8080
ENV IAZIO_EXTRACT_WORKER=native
ENTRYPOINT ["/usr/local/bin/iazio-harness-api"]
