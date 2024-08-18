package s3pmoxcommon

import "github.com/minio/minio-go/v7"

var PROXMOX_INDEX_MAGIC_DYNAMIC = [8]byte{28, 145, 78, 165, 25, 186, 179, 205}

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
	C          *minio.Client
	Datastore  string
	corrupted  bool
}
