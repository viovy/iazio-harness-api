# syntax=docker/dockerfile:1

FROM python:3.12-bookworm AS worker
RUN apt-get update \
  && apt-get install -y --no-install-recommends chromium chromium-driver python3-selenium \
  && rm -rf /var/lib/apt/lists/*

FROM golang:1.26-bookworm AS build
ARG GIT_VERSION=0.1.0-dev
ARG GIT_COMMIT=unknown
WORKDIR /src
COPY go.mod ./
COPY . .
RUN CGO_ENABLED=0 go build -trimpath \
  -ldflags="-s -w -X github.com/viovy/iazio-harness-api/internal/version.Version=${GIT_VERSION} -X github.com/viovy/iazio-harness-api/internal/version.Commit=${GIT_COMMIT}" \
  -o /out/iazio-harness-api ./cmd/iazio-harness-api

FROM worker
RUN useradd --create-home --uid 10001 app
COPY --from=build /out/iazio-harness-api /usr/local/bin/iazio-harness-api
COPY internal/extract/gemini_worker.py /opt/extract/gemini_worker.py
USER app
EXPOSE 8080
ENV IAZIO_EXTRACT_WORKER=/opt/extract/gemini_worker.py
ENTRYPOINT ["/usr/local/bin/iazio-harness-api"]
