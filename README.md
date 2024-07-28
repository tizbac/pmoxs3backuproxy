WIP!! 
Use as follows

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


The on PVE add proxmox backup server storage 

127.0.0.1:8007 

Use 55:BC:29:4B:BA:B6:A1:03:42:A9:D8:51:14:9D:BD:00:D2:2A:9C:A1:B8:4A:85:E1:AF:B2:0C:48:40:D6:CC:A4 as fingerprint, if you intend to bind on public network with potential MITM, please regenerate server certificate !!

Use apikey@pbs for username, s3 secret as password , and bucket as datastore 

Does currently only work for pbs VM backups, not with proxmox-backup-client
https://github.com/tizbac/pmoxs3backuproxy/issues/2 
