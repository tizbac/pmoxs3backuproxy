package s3pmoxcommon

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
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
	for object := range c.ListObjects(
		ctx, datastore,
		minio.ListObjectsOptions{Recursive: true, Prefix: "backups/",
			WithMetadata: true,
		}) {
		//The object name is backupid|unixtimestamp|type
		path := strings.Split(object.Key, "/")
		if strings.Count(object.Key, "/") == 2 {
			fields := strings.Split(path[1], "|")
			existing_S, ok := prefixMap[path[1]]
			if ok {
				if len(path) == 3 {
					/** Dont add the custom chunk index list
					 * for dynamic backup to filelist
					 **/
					if strings.HasSuffix(path[2], ".csjson") {
						continue
					}
					if object.UserTags["protected"] == "true" {
						existing_S.Protected = true
					}
					if object.UserTags["note"] != "" {
						note, _ := base64.RawStdEncoding.DecodeString(object.UserTags["note"])
						existing_S.Comment = string(note)
					}
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
				Datastore:  datastore,
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

func GetLatestSnapshot(c minio.Client, ds string, id string) (*Snapshot, error) {
	snapshots, err := ListSnapshots(c, ds, false)
	if err != nil {
		s3backuplog.ErrorPrint(err.Error())
		return nil, err
	}

	if len(snapshots) == 0 {
		return nil, nil
	}

	var mostRecent = &Snapshot{}
	mostRecent = nil
	for _, sl := range snapshots {
		if (mostRecent == nil || sl.BackupTime > mostRecent.BackupTime) && id == sl.BackupID {
			mostRecent = &sl
		}
	}

	if mostRecent == nil {
		return nil, nil
	}

	return mostRecent, nil
}

func (S *Snapshot) InitWithQuery(v url.Values) {
	S.BackupID = v.Get("backup-id")
	S.BackupTime, _ = strconv.ParseUint(v.Get("backup-time"), 10, 64)
	S.BackupType = v.Get("backup-type")
}

func (S *Snapshot) InitWithForm(r *http.Request) {
	S.BackupID = r.FormValue("backup-id")
	S.BackupTime, _ = strconv.ParseUint(r.FormValue("backup-time"), 10, 64)
	S.BackupType = r.FormValue("backup-type")
}

func (S *Snapshot) S3Prefix() string {
	return fmt.Sprintf("backups/%s|%d|%s", S.BackupID, S.BackupTime, S.BackupType)
}

func (S *Snapshot) GetFiles(c minio.Client) {
	for object := range c.ListObjects(
		context.Background(), S.Datastore,
		minio.ListObjectsOptions{Recursive: true, Prefix: S.S3Prefix()},
	) {
		file := SnapshotFile{}
		path := strings.Split(object.Key, "/")
		file.Filename = path[2]
		file.Size = uint64(object.Size)
		S.Files = append(S.Files, file)
	}
}

func (S *Snapshot) ReadTags(c minio.Client) (map[string]string, error) {
	existingTags, err := c.GetObjectTagging(
		context.Background(),
		S.Datastore,
		S.S3Prefix()+"/index.json.blob",
		minio.GetObjectTaggingOptions{},
	)
	if err != nil {
		s3backuplog.ErrorPrint("Unable to get tags: %s", err.Error())
		return nil, err
	}
	return existingTags.ToMap(), nil
}

func (S *Snapshot) Delete(c minio.Client) error {
	objectsCh := make(chan minio.ObjectInfo)
	go func() {
		defer close(objectsCh)
		// List all objects from a bucket-name with a matching prefix.
		opts := minio.ListObjectsOptions{Prefix: S.S3Prefix(), Recursive: true}
		for object := range c.ListObjects(context.Background(), S.Datastore, opts) {
			if object.Err != nil {
				s3backuplog.ErrorPrint(object.Err.Error())
			}
			objectsCh <- object
		}
	}()
	errorCh := c.RemoveObjects(context.Background(), S.Datastore, objectsCh, minio.RemoveObjectsOptions{})
	for e := range errorCh {
		s3backuplog.ErrorPrint("Failed to remove " + e.ObjectName + ", error: " + e.Err.Error())
		return e.Err
	}
	return nil
}

func GetLookupType(Typeflag string) minio.BucketLookupType {
	switch Typeflag {
	case "path":
		return minio.BucketLookupPath
	case "dns":
		return minio.BucketLookupDNS
	default:
		return minio.BucketLookupAuto
	}
}
