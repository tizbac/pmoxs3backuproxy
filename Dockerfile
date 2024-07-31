FROM golang:1.22 AS builder
ARG TARGETOS
ARG TARGETARCH

WORKDIR /workspace

# Copy the Go Modules manifests
COPY go.mod go.mod
COPY go.sum go.sum

# cache deps before building and copying source so that we don't need to re-download as much
# and so that source changes don't invalidate our downloaded layer
RUN go mod download

# Copy the go source
COPY . .

# Build
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} go build -o pmoxs3backuproxy

# Use chainguard static image as a minimal base
# Refer to https://images.chainguard.dev/directory/image/static/versions for more details
FROM cgr.dev/chainguard/static:latest

WORKDIR /

COPY --from=builder /workspace/pmoxs3backuproxy .

COPY server.crt /
COPY server.key /

USER 65532:65532

EXPOSE 8007

ENTRYPOINT ["/pmoxs3backuproxy"]
