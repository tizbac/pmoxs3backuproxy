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
FROM debian:bookworm-slim
ARG source="https://github.com/tizbac/pmoxs3backuproxy"
ARG goversion="https://go.dev/dl/go1.22.5.linux-amd64.tar.gz"
LABEL container.name="pmoxs3backuproxy"
LABEL container.description="Proxy written in golang that will emulate a PBS server and work on one or more S3 buckets"
LABEL container.source=$source
LABEL container.version="1.1"
LABEL maintainer="Michael Ablassmeier <abi@grinser.de>"

COPY . /tmp/build/

# Deploys dependencies and pulls sources, installing virtnbdbackup and removing unnecessary content:
RUN \
export PATH=$PATH:/usr/local/go/bin && \
apt-get update && \
apt-get install -y --no-install-recommends curl ca-certificates && \
curl -sL $goversion | tar -C /usr/local -zxf - && \
cd /tmp/build/ && go build . && cp pmoxs3backuproxy /usr/bin && \
cp server.key server.crt /root/ && \
rm -rf /var/lib/apt/lists/* /tmp/* /usr/local/go

# Default folder:
WORKDIR /root
CMD pmoxs3backuproxy
