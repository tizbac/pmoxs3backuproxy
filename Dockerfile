# Dockefile
#
# Build:
#  docker build . --tag=pmoxs3backuproxy
# Usage:
#  docker run -it  pmoxs3backuproxy:latest
# Run with endpoint set to 127.0.0.1:9000:
#  docker run -it  pmoxs3backuproxy:latest pmoxs3backuproxy -endpoint 127.0.0.1:9000
#
# Note: uses default server key and certificate stored in /root/
FROM golang:1.22.5 as build
ARG source="https://github.com/tizbac/pmoxs3backuproxy"
LABEL container.name="pmoxs3backuproxy"
LABEL container.description="Proxy written in golang that will emulate a PBS server and work on one or more S3 buckets"
LABEL container.source=$source
LABEL container.version="0.1"
COPY . .
RUN go mod download
RUN CGO_ENABLED=0 go build -o /go/bin/pmoxs3backuproxy
RUN cp server.key /tmp/
RUN cp server.crt /tmp/

FROM gcr.io/distroless/static-debian12
COPY --from=build /go/bin/pmoxs3backuproxy /usr/bin/
COPY --from=build /tmp/server.key /
COPY --from=build /tmp/server.crt /

# Default folder:
CMD ["/usr/bin/pmoxs3backuproxy"]
