package main

import (
	"context"
	"net"
	"tizbac/pmoxs3backuproxy/internal/s3backuplog"

	"github.com/minio/minio-go/v7"
	"golang.org/x/net/http2"
)

func (s *Server) backup(sock net.Conn, C TicketEntry, ds string, S Snapshot) {
	srv := &http2.Server{}
	//We serve the HTTP2 connection back using default handler after protocol upgrade
	snew := &Server{Auth: make(map[string]TicketEntry), H2Ticket: &C, SelectedDataStore: &ds, Snapshot: &S, Writers: make(map[int32]*Writer), Finished: false}
	srv.ServeConn(sock, &http2.ServeConnOpts{Handler: snew})
	if !snew.Finished { //Incomplete backup because connection died pve side, remove from S3
		s3backuplog.WarnPrint("Removing incomplete backup %s", snew.Snapshot.S3Prefix())
		objectsCh := make(chan minio.ObjectInfo)
		go func() {
			defer close(objectsCh)
			// List all objects from a bucket-name with a matching prefix.
			opts := minio.ListObjectsOptions{Prefix: S.S3Prefix(), Recursive: true}
			for object := range C.Client.ListObjects(context.Background(), ds, opts) {
				if object.Err != nil {
					s3backuplog.ErrorPrint(object.Err.Error())
				}
				objectsCh <- object
			}
		}()
		errorCh := C.Client.RemoveObjects(context.Background(), ds, objectsCh, minio.RemoveObjectsOptions{})
		for e := range errorCh {
			s3backuplog.ErrorPrint("Failed to remove " + e.ObjectName + ", error: " + e.Err.Error())
		}
	}
}

func (s *Server) restore(sock net.Conn, C TicketEntry, ds string, S Snapshot) {
	srv := &http2.Server{}
	//We serve the HTTP2 connection back using default handler after protocol upgrade
	snew := &Server{Auth: make(map[string]TicketEntry), H2Ticket: &C, SelectedDataStore: &ds, Snapshot: &S, Writers: make(map[int32]*Writer), Finished: false}
	srv.ServeConn(sock, &http2.ServeConnOpts{Handler: snew})
}
