package s3pmoxcommon

var PROXMOX_INDEX_MAGIC_DYNAMIC = [8]byte{28, 145, 78, 165, 25, 186, 179, 205}
var PROXMOX_INDEX_MAGIC_STATIC = [8]byte{47, 127, 65, 237, 145, 253, 15, 205}

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
	Datastore  string
	corrupted  bool
	Size       uint64 `json:"size"`
}
