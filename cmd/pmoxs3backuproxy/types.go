package main

import (
	"sync"
	"tizbac/pmoxs3backuproxy/internal/s3pmoxcommon"

	"github.com/juju/mutex/v2"
	"github.com/minio/minio-go/v7"
)

type AuthTicketResponsePayload struct {
	CSRFPreventionToken string `json:"CSRFPreventionToken"`
	Ticket              string `json:"ticket"`
	Username            string `json:"username"`
}

type AccessTicketRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type DataStore struct {
	Store string `json:"store"`
}

type TicketEntry struct {
	AccessKeyID     string
	SecretAccessKey string
	Endpoint        string
	Client          *minio.Client
	Expire          uint64
}

type Writer struct {
	FidxName    string
	Assignments map[int64][]byte
	Chunksize   uint64
	Size        uint64
	ReuseCSUM   string
}

type Server struct {
	Auth              sync.Map
	H2Ticket          *TicketEntry
	SelectedDataStore *string
	Snapshot          *s3pmoxcommon.Snapshot
	Writers           map[int32]*Writer
	CurWriter         int32
	Finished          bool
	S3Endpoint        string
	SecureFlag        bool
	TicketExpire      uint64
	Sessions          uint64
	SessionsMutex     sync.Mutex
	SessionsRelease   mutex.Releaser
}

type DataStoreStatus struct {
	Avail int64 `json:"avail"`
	Total int64 `json:"total"`
	Used  int64 `json:"used"`
}

type AssignmentRequest struct {
	DigestList []string `json:"digest-list"`
	OffsetList []uint64 `json:"offset-list"`
	Wid        int32    `json:"wid"`
}

type FixedIndexCloseRequest struct {
	ChunkCount int64  `json:"chunk-count"`
	CSum       string `json:"csum"`
	Wid        int32  `json:"wid"`
	Size       int64  `json:"size"`
}

type Response struct {
	Data interface{} `json:"data"`
	// other fields
}
