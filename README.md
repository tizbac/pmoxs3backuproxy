<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**

- [Working Features](#working-features)
- [Known issues](#known-issues)
  - [S3 Restore Performance / Considerations](#s3-restore-performance--considerations)
  - [Sizes shown in PVE frontend](#sizes-shown-in-pve-frontend)
- [Usage](#usage)
- [Quickstart](#quickstart)
    - [Setup minio](#setup-minio)
    - [PVE configuration: direct backup](#pve-configuration-direct-backup)
    - [PBS configuration: using the proxy as remote](#pbs-configuration-using-the-proxy-as-remote)
    - [Proxmox backup client](#proxmox-backup-client)
- [Running with Docker](#running-with-docker)
- [Notes](#notes)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

WIP!! 
Use as follows

Note: Garbage collector is experimental, use with extreme caution

# Working Features

The following features are currently implemented:

 * As with PBS 3.3+ you can use the PBS [push and pull features](https://pbs.proxmox.com/docs/managing-remotes.html) for syncing your
   backups to/from S3 compatible backends.
 * Configure proxy in PVE and use it for both CT and VM backups (full and incremental)
 * Restore functionality (VM restore, mount, map)
 * Basic PVE UI integration: adding notes, setting the protection flag,
   removing backups, showing configuration.
 * File backup/restore/mount via proxmox-backup-client (full and incremental)

# Known issues
## S3 Restore Performance / Considerations

Both the proxmox VM and file backup client will split the backups into many
small chunks, S3 is not known to perform well upon reading many small files.

If using the proxy as direct datastore, the more likely you will see
performance issues during restore, depending on the amount of objects stored.

Using the PBS Push or Pull functions can greatly enhance speeds to sync your
backups, as they are done in parallel.

Also think about cost peaks for object read operations. The proxmox backup
clients will request required chunks sequentially, there is currently no
way to optimize this in the proxy.

You should consider twice if you want to make the S3 backend your primary
backup storage. For local S3 instances with S3 compatible API (ceph, minio) the
performance depends largely on your setup.

As with proxmox 8.2, a feature called "Backup fleecing" was introduced, [See
release notes](https://pve.proxmox.com/wiki/Roadmap#Proxmox_VE_8.2) which
prevents VM lockup / slow down in case of slow backup storage, which is more
likely to happen with hosted S3 over slow network connections.

## Sizes shown in PVE frontend

The backup size is currently not correctly shown

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
  -lookuptype string
        Bucket lookup type: auto,dns,path (default: "auto")
  -usessl
        Enable SSL connection to the endpoint, for use with cloud S3 providers
```

```
Usage of ./garbagecollector:
  -accesskey string
        S3 Access Key ID
  -bucket string
        Bucket to perform garbage collection on
  -debug
        Debug logging
  -endpoint string
        S3 Endpoint without https/http , host:port
  -lookuptype string
        Bucket lookup type: auto,dns,path (default: "auto")
  -retention uint
        Number of days to keep backups for (default 60)
  -secretkey string
        S3 Secret Key, discouraged , use a file if possible
  -secretkeyfile string
        S3 Secret Key File
  -usessl
        Use SSL for endpoint connection: default: false

```

# Quickstart
### Setup minio

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

### PVE configuration: direct backup

Add the proxy endpoint as proxmox backup server storage using the following address:

`127.0.0.1:8007`

Use

```
55:BC:29:4B:BA:B6:A1:03:42:A9:D8:51:14:9D:BD:00:D2:2A:9C:A1:B8:4A:85:E1:AF:B2:0C:48:40:D6:CC:A4
```

`Note:` if you intend to run the proxy on a public network, please replace the
included certificates with your own to prevent possible MITM attacks.

Use the created access_key@pbs for username and secret key as password, bucket
as datastore


### PBS configuration: using the proxy as remote

If you want to use the proxy as remote datastore in PBS, configure it as
described in the [proxmox manual](https://pbs.proxmox.com/docs/managing-remotes.html)


### Proxmox backup client

Use proxmox backup client by setting the repository and password accordingly:

```
 export PBS_PASSWORD=<secret>
 proxmox-backup-client backup root.pxar:/etc --repository <access_key>@pbs@127.0.0.1:<bucket>
```

# Running with Docker

Add the following to your `docker-compose.yml`, add/update your `-endpoint`, then run `docker compose up -d`. The service will be accessible at `localhost:8007`.
```
name: pmoxs3backuproxy
services:
  pmoxs3backuproxy:
    image: ghcr.io/tizbac/pmoxs3backuproxy:latest
    command: -bind 127.0.0.1:8007 -endpoint 127.0.0.1:9000
    container_name: pmoxs3backuproxy
    hostname: pmoxs3backuproxy
    restart: unless-stopped
    volumes:
      - /etc/localtime:/etc/localtime:ro
    ports:
      - '8007:8007'
```

For increased security, you can add the following security parameters without affecting container function:
```
    user: '65532:65532'
    security_opt:
      - no-new-privileges:true
    cap_drop:
      - ALL
```
To execute the garbage collector set a different entrypoint:
```
 docker run --entrypoint /garbagecollector -it ghcr.io/tizbac/pmoxs3backuproxy:latest
```

# Notes

Garbage collector process ( scheduled with crontab ) must absolutely run on
same machine as the proxy for locking to work!

Garbage collector will also check for integrity ( only the presence of all
referenced chunks ), if a backup is found to be broken, it will not be deleted
and retention will be honored, but it will be marked corrupted, so next backup
from PVE will be non incremental and will recreate missing chunk if needed.
Corrupted backup will not appear in PVE backup list
