<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**

- [Usage](#usage)
- [Quickstart](#quickstart)
    - [minio](#minio)
- [PVE configuration](#pve-configuration)
- [Notes](#notes)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

WIP!! 
Use as follows

# Usage

```
Usage of ./pmoxs3backuproxy:
  -bind string
        PBS Protocol bind address, recommended 127.0.0.1:8007, use :8007 for all (default "127.0.0.1:8007")
  -cert string
        Server SSL certificate file (default "server.crt")
  -debug
        Debug logging
  -endpoint string
        S3 Endpoint without https/http , host:port
  -key string
        Server SSL key file (default "server.key")
```

# Quickstart
### minio

Start minio server (either on the PVE system or on a remote system),
specify the listening IP via `--address`

```
minio server ~/minio --address 127.0.0.1:9000
mc alias set 'myminio' 'http://127.0.0.1:9000' 'minioadmin' 'minioadmin'
```

Create the target bucket used as datastore:

```
mc mb myminio/backups
```

Create an API access key for the minio admin account:

```
mc admin user svcacct add myminio minioadmin
Access Key: 431EM4CTA0OP810W6FER
Secret Key: RIa82lyl6ZrEYVtvwaMgh2JFlOISENiGQT+Lv0IE
Expiration: no-expiry
```

Start the proxy via:

```
pmoxs3backuproxy -endpoint 127.0.0.1:9000
```

# PVE configuration

The on PVE add proxmox backup server storage 

127.0.0.1:8007 

Use
55:BC:29:4B:BA:B6:A1:03:42:A9:D8:51:14:9D:BD:00:D2:2A:9C:A1:B8:4A:85:E1:AF:B2:0C:48:40:D6:CC:A4
as fingerprint, if you intend to bind on public network with potential MITM,
please regenerate server certificate !!

Use the created access_key@pbs for username, and secret key as password, and
bucket as datastore

# Notes

Does currently only work for pbs VM backups, not with proxmox-backup-client
https://github.com/tizbac/pmoxs3backuproxy/issues/2 
