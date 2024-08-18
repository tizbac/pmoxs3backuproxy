package s3pmoxcommon

import "github.com/minio/minio-go/v7"

type SnapshotFile struct {
	Filename  string `json:"filename"`
	CryptMode string `json:"crypt-mode"` //none
	Size      uint64 `json:"size"`
}

type Snapshot struct {
	BackupID   string         `json:"backup-id"`
	BackupTime uint64         `json:"backup-time"`
	BackupType string         `json:"backup-type"` // vm , ct, host
	Files      []SnapshotFile `json:"files"`
	Protected  bool           `json:"protected"`
	Comment    string         `json:"comment"` // first line of notes
	C          *minio.Client
	Datastore  string
	corrupted  bool
}
