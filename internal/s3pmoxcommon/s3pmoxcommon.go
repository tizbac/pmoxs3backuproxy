package s3pmoxcommon

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"tizbac/pmoxs3backuproxy/internal/s3backuplog"

	"github.com/minio/minio-go/v7"
)

func ListSnapshots(c minio.Client, datastore string, returnCorrupted bool) ([]Snapshot, error) {
	resparray := make([]Snapshot, 0)
	resparray2 := make([]Snapshot, 0)
	prefixMap := make(map[string]*Snapshot)
	ctx := context.Background()
	for object := range c.ListObjects(ctx, datastore, minio.ListObjectsOptions{Recursive: true, Prefix: "backups/"}) {
		//log.Println(object.Key)
		//The object name is backupid|unixtimestamp|type
		path := strings.Split(object.Key, "/")
		if strings.Count(object.Key, "/") == 2 {

			fields := strings.Split(path[1], "|")
			existing_S, ok := prefixMap[path[1]]
			if ok {
				//log.Println(path)
				if len(path) == 3 {
					existing_S.Files = append(existing_S.Files, SnapshotFile{
						Filename:  path[2],
						CryptMode: "none", //TODO
						Size:      uint64(object.Size),
					})
				}
				continue
			}
			backupid := fields[0]
			backuptime := fields[1]
			backuptype := fields[2]
			backuptimei, _ := strconv.ParseUint(backuptime, 10, 64)
			S := Snapshot{
				BackupID:   backupid,
				BackupTime: backuptimei,
				BackupType: backuptype,
				Files:      make([]SnapshotFile, 0),
				c:          &c,
				datastore:  datastore,
				corrupted:  false,
			}
			if len(path) == 3 {
				S.Files = append(S.Files, SnapshotFile{
					Filename:  path[2],
					CryptMode: "none", //TODO
					Size:      uint64(object.Size),
				})
			}

			resparray = append(resparray, S)
			prefixMap[path[1]] = &resparray[len(resparray)-1]

		}

		if strings.HasSuffix(object.Key, "/corrupted") {
			prefixMap[path[1]].corrupted = true
		}
	}
	for _, s := range resparray {
		if returnCorrupted || !s.corrupted {
			resparray2 = append(resparray2, s)
		}
	}
	return resparray2, ctx.Err()
}

func (S *Snapshot) InitWithQuery(v url.Values) {
	S.BackupID = v.Get("backup-id")
	S.BackupTime, _ = strconv.ParseUint(v.Get("backup-time"), 10, 64)
	S.BackupType = v.Get("backup-type")
	S.Protected = false
}

func (S *Snapshot) S3Prefix() string {
	return fmt.Sprintf("backups/%s|%d|%s", S.BackupID, S.BackupTime, S.BackupType)
}

func (S *Snapshot) Delete() error {
	objectsCh := make(chan minio.ObjectInfo)
	go func() {
		defer close(objectsCh)
		// List all objects from a bucket-name with a matching prefix.
		opts := minio.ListObjectsOptions{Prefix: S.S3Prefix(), Recursive: true}
		for object := range S.c.ListObjects(context.Background(), S.datastore, opts) {
			if object.Err != nil {
				s3backuplog.ErrorPrint(object.Err.Error())
			}
			objectsCh <- object
		}
	}()
	errorCh := S.c.RemoveObjects(context.Background(), S.datastore, objectsCh, minio.RemoveObjectsOptions{})
	for e := range errorCh {
		s3backuplog.ErrorPrint("Failed to remove " + e.ObjectName + ", error: " + e.Err.Error())
		return e.Err
	}
	return nil
}
